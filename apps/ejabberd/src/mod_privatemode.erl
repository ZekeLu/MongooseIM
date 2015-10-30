%%%===================================================================
%%% @doc Private mode: to mark a contact or group as private
%%%     The client should protect a user's conversation with a private
%%%     contact or group.
%%%     https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#private-mode
%%%===================================================================
-module(mod_privatemode).

-behaviour(gen_mod).

%% gen_mod callbacks
-export([start/2, stop/1]).

%% IQ handlers
-export([process_iq/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(NS_PRIVATEMODE, <<"aft:privatemode">>).


%%%===================================================================
%%% gen_mod callbacks
%%%===================================================================

start(Host, _Opts) ->
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host,
        ?NS_PRIVATEMODE, ?MODULE, process_iq, no_queue).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_PRIVATEMODE).


%%%===================================================================
%%% IQ handlers
%%%===================================================================

process_iq(From, To, #iq{xmlns = ?NS_PRIVATEMODE, type = _Type, sub_el = SubEl} = IQ) ->
    case is_query(SubEl) of
        true ->
            case xml:get_tag_attr_s(<<"query_type">>, SubEl) of
                <<"contact">> ->
                    set_contact(From, To, IQ);
                <<"group">> ->
                    set_group(From, To, IQ);
                <<"set_password">> ->
                    set_password(From, To, IQ);
                <<"login">> ->
                    privatemode_login(From, To, IQ);
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
            end;
        false ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;

process_iq(_, _, IQ) ->
    #iq{sub_el = SubEl} = IQ,
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc to mark a contact as private or not private
set_contact(From, _To, #iq{sub_el = SubEl} = IQ) ->
    #jid{luser = LUser, lserver = LServer} = From,
    ContactJid = xml:get_tag_attr_s(<<"subject">>, SubEl),
    Private = xml:get_tag_attr_s(<<"private">>, SubEl),
    case odbc_privatemode:set_contact(LServer, LUser, ContactJid, Private) of
        ok ->
            multiple_resources_broadcast(LUser, LServer, ContactJid, Private, <<"contact">>),
            IQ#iq{type = result, sub_el = [SubEl]};
        _ ->
            IQ#iq{type = error, sub_el = [SubEl]}
    end.

%% @doc to mark a group as private or not private
set_group(From, _To, #iq{sub_el = SubEl} = IQ) ->
    #jid{luser = LUser, lserver = LServer} = From,
    GroupId = xml:get_tag_attr_s(<<"subject">>, SubEl),
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    Private = xml:get_tag_attr_s(<<"private">>, SubEl),
    case odbc_privatemode:set_group(LServer, UserJid, GroupId, Private) of
        ok ->
            multiple_resources_broadcast(LUser, LServer, GroupId, Private, <<"group">>),
            IQ#iq{type = result, sub_el = [SubEl]};
        _ ->
            IQ#iq{type = error, sub_el = [SubEl]}
    end.

multiple_resources_broadcast(LUser, LServer, ContactJid, Private, Mode) ->
    FromJid = jlib:make_jid(<<>>, <<>>, <<>>),
    ToJid = jlib:make_jid(LUser, LServer, <<>>),
    LangAttr = {<<"xml:lang">>, <<"en">>},
    ToAttr = {<<"to">>, jlib:jid_to_binary(ToJid)},
    TypeAttr = {<<"type">>, <<"chat">>},
    Packet = {xmlel, <<"message">>, [],
        [{xmlel, <<"push">>, [{<<"xmlns">>, ?NS_PRIVATEMODE}, {<<"type">>, <<"privatemode">>},
            {<<"mode">>, Mode}, {<<"subject">>, ContactJid}, {<<"private">>, Private}],
            [{xmlcdata, <<>>}]}
        ]},
    ejabberd_router:route(FromJid, ToJid,
        Packet#xmlel{attrs = [ToAttr, TypeAttr, LangAttr]}).

%% @doc set privatemode password
set_password(From, _To, #iq{sub_el = SubEl} = IQ) ->
    #jid{luser = LUser, lserver = LServer} = From,
    FromJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    OldPassword = get_password_SubEl(SubEl, <<"oldpassword">>),
    case is_password_correct(LServer, FromJid, OldPassword) of
        false ->
            IQ#iq{type = error, sub_el = [SubEl, make_error_reply(<<"password incorrect">>)]};
        _ ->
            NewPassword = get_password_SubEl(SubEl, <<"password">>),
            case NewPassword of
                not_exists ->
                    IQ#iq{type = error, sub_el = [SubEl, make_error_reply(<<"password not exists">>)]};
                _ ->
                    PreparedPassword = password_to_scram(NewPassword),
                    case odbc_privatemode:set_password(LServer, FromJid, PreparedPassword) of
                        ok ->
                            IQ#iq{type = result, sub_el = [SubEl]};
                        _ ->
                            IQ#iq{type = error, sub_el = [SubEl, make_error_reply(<<"set password failed">>)]}
                    end
            end
    end.

%% @doc login privatemode
privatemode_login(From, _To, #iq{sub_el = SubEl} = IQ) ->
    #jid{luser = LUser, lserver = LServer} = From,
    FromJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    Password = get_password_SubEl(SubEl, <<"password">>),
    case is_password_correct(LServer, FromJid, Password) of
        true ->
            IQ#iq{type = result, sub_el = [SubEl]};
        not_set ->
            IQ#iq{type = error, sub_el = [SubEl, make_error_reply(<<"password not set">>)]};
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, make_error_reply(<<"password incorrect">>)]}
    end.

is_password_correct(LServer, Jid, Password) ->
    case odbc_privatemode:get_password(LServer, Jid) of
        {ok, [{PassDetails}]} ->
            case check_password_scram(Password, PassDetails) of
                true -> true;
                _ -> false
            end;
        {ok, []} ->
            not_set;
        _ ->
            false
    end.

make_error_reply(Error) ->
    #xmlel{name = <<"error">>
        , attrs = []
        , children = [{xmlcdata, Error}]
    }.

get_password_SubEl(SubEl, TagName) ->
    Tag = xml:get_subtag(SubEl, TagName),
    case Tag of
        false ->
            not_exists;
        _ ->
            xml:get_tag_cdata(Tag)
    end.

password_to_scram(Password) ->
    Scram = scram:password_to_scram(Password, 4096),
    case scram:serialize(Scram) of
        {error, _} ->
            false;
        PassDetails ->
            ejabberd_odbc:escape(PassDetails)
    end.

check_password_scram(Password, PassDetails) ->
    case scram:deserialize(PassDetails) of
        {ok, #scram{} = Scram} ->
            scram:check_password(Password, Scram);
        _ ->
            false %% Password is not correct
    end.

is_query(Packet) ->
    case Packet of
        #xmlel{name = <<"query">>} -> true;
        _ -> false
    end.
