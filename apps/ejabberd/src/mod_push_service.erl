%%%===================================================================
%%% @doc Push Notification Service Module
%%% * Module Detail:
%%%   1. handler all offline message with a hook;
%%%   2. extract chat content from stanza;
%%%   3. get token records by user jid;
%%%   4. call different push channels which dependent on push type in token records.
%%% * Push Type:
%%%   1. iOS
%%%   2. Android
%%%   3. Other
%%%===================================================================

-module(mod_push_service).

-behaviour(gen_mod).

%% gen_mod callbacks
-export([start/2,
    init/2,
    stop/1]).

%% hooks
-export([send_notification/3]).

%% exports
-export([get_host_server/0]).


-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_push_service.hrl").

start(Host, Opts) ->
    ?INFO_MSG("Starting mod_push_service", []),
    register(?MODULE, spawn(?MODULE, init, [Host, Opts])),
    ok.

init(Host, _Opts) ->
    ejabberd_hooks:add(offline_message_hook, Host, ?MODULE, send_notification, 10),
    ok.

stop(Host) ->
    ?INFO_MSG("Stopping mod_push_service", []),
    ejabberd_hooks:delete(offline_message_hook, Host,
        ?MODULE, send_notification, 10),
    ok.

%%%===================================================================
%%% hooks
%%%===================================================================
-spec send_notification(
    From :: jlib:jid(),
    ToJid :: jlib:jid(),
    Packet :: jlib:xmlel()) -> ok.
send_notification(From, To, Packet) ->
    case extract_message(From, To, Packet) of
        not_valid_message -> none;
        #push_message{
            lserver = LServer,
            tojid = ToJid
        } = Message ->
            PushContent = make_push_message(Message),
            ejabberd_hooks:run(push_service_hook,[LServer,ToJid,PushContent])
    end.


%% ========================================================
%% helpers
%% ========================================================

-spec extract_message(
    From :: jlib:jid(),
    To :: jlib:jid(),
    Packet :: jlib:xmlel()) -> {#push_message{} | not_valid_message}.
extract_message(From, To, Packet) ->
    #jid{luser = LUser, lserver = LServer} = To,
    ToJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    #jid{luser = LUserFrom, lserver = LServerFrom} = From,
    FromJid = jlib:jid_to_binary({LUserFrom, LServerFrom, <<>>}),

    InfoTag = xml:get_subtag(Packet, <<"info">>),
    case InfoTag of
        false ->
            not_valid_message;
        _ ->
            case xml:get_tag_attr_s(<<"xmlns">>, InfoTag) of
                <<"aft:message">> ->
                    Lang = case xml:get_tag_attr_s(<<"xml:lang">>, Packet) of
                               false -> "en";
                               TLang -> TLang
                           end,
                    Message = case {xml:get_tag_attr_s(<<"groupChat">>, InfoTag),
                        xml:get_subtag(InfoTag, <<"sender">>)} of
                                  {<<"1">>, false} ->
                                      not_valid_message;
                                  {<<"1">>, SenderTag} ->
                                      Sender = xml:get_tag_cdata(SenderTag),
                                      #jid{luser = SenderUser} = jlib:binary_to_jid(Sender),
                                      #push_message{
                                          lang = Lang,
                                          lserver = LServer,
                                          fromuser = SenderUser,
                                          fromjid = Sender,
                                          touser = LUser,
                                          tojid = ToJid,
                                          groupid = LUserFrom,
                                          content = undefined
                                      };
                                  {<<"0">>, _} ->
                                      #push_message{
                                          lang = Lang,
                                          lserver = LServer,
                                          fromuser = LUserFrom,
                                          fromjid = FromJid,
                                          touser = LUser,
                                          tojid = ToJid,
                                          groupid = undefined,
                                          content = undefined
                                      };
                                  _ ->
                                      not_valid_message
                              end,
                    case Message of
                        not_valid_message -> not_valid_message;
                        _ ->
                            case xml:get_tag_attr_s(<<"type">>, InfoTag) of
                                <<"0">> ->
                                    case xml:get_subtag(InfoTag, <<"text">>) of
                                        false -> not_valid_message;
                                        TextTag ->
                                            Content = xml:get_tag_cdata(TextTag),
                                            Message#push_message{
                                                type = text,
                                                content = Content
                                            }
                                    end;
                                <<"1">> ->
                                    Message#push_message{
                                        type = audio
                                    };
                                <<"2">> ->
                                    Message#push_message{
                                        type = video
                                    };
                                <<"3">> ->
                                    Message#push_message{
                                        type = picture
                                    };
                                <<"4">> ->
                                    Message#push_message{
                                        type = location
                                    };
                                _ ->
                                    Message#push_message{
                                        type = undefined
                                    }
                            end
                    end;
                _ ->
                    not_valid_message
            end
    end.

-spec make_push_message(
    #push_message{}) -> binary().
make_push_message(#push_message{
    lang = Lang,
    lserver = LServer,
    fromjid = FromJid,
    touser = ToUser,
    type = MessageType,
    content = Content,
    groupid = GroupId
}) ->
    {Nickname, GroupTitle} = case GroupId of
                                 undefined ->
                                     {get_roster_nick(Lang, LServer, FromJid, ToUser), <<>>};
                                 _ ->
                                     GroupName = get_group_name(Lang, LServer, GroupId),
                                     {get_group_nick(Lang, LServer, FromJid, GroupId), <<"[", GroupName/binary, "]">>}
                             end,
    HeadTitle = <<GroupTitle/binary, Nickname/binary>>,
    case MessageType of
        text ->
            case Content of
                undefined ->
                    translate(Lang, "~s send a message to you", [HeadTitle]);
                _ ->
                    SubContent = message_substring(Content),
                    <<HeadTitle/binary, ": ", SubContent/binary>>
            end;
        audio ->
            translate(Lang, "~s send an audio to you", [HeadTitle]);
        video ->
            translate(Lang, "~s send a video to you", [HeadTitle]);
        picture ->
            translate(Lang, "~s send a picture to you", [HeadTitle]);
        location ->
            translate(Lang, "~s share location with you", [HeadTitle]);
        _ ->
            translate(Lang, "~s send a message to you", [HeadTitle])
    end.

-spec get_roster_nick(Lang :: ejabberd:lang(), LServer :: binary(), FromJid :: binary(), ToJid :: binary()) -> binary().
get_roster_nick(Lang, LServer, FromJid, ToUser) ->
    case odbc_push_service:get_roster_nick(LServer, FromJid, ToUser) of
        {ok, Name} -> Name;
        _ -> translate(Lang, "unknown")
    end.

-spec get_group_nick(Lang :: ejabberd:lang(), LServer :: binary(), ToJid :: binary(), GroupId :: binary()) -> binary().
get_group_nick(Lang, LServer, FromJid, GroupId) ->
    case odbc_push_service:get_group_nick(LServer, FromJid, GroupId) of
        {ok, Name} -> Name;
        _ -> translate(Lang, "unknown")
    end.

-spec get_group_name(Lang :: ejabberd:lang(), LServer :: binary(), GroupId :: binary()) -> binary().
get_group_name(Lang, LServer, GroupId) ->
    case odbc_push_service:get_group_name(LServer, GroupId) of
        {ok, Name} -> Name;
        _ -> translate(Lang, "unknown")
    end.

-spec message_substring(Content :: binary()) -> binary().
message_substring(Content) ->
    Size = byte_size(Content),
    case Size > 100 of
        true ->
            SubContent = binary:part(Content, {0, 100}),
            <<SubContent/binary, "...">>;
        false ->
            Content
    end.

-spec translate(Lang :: ejabberd:lang(), Text :: binary()) -> binary().
translate(Lang, Text) ->
    iolist_to_binary(translate:translate(Lang, Text)).

-spec translate(Lang :: ejabberd:lang(), Text :: binary(), Replace :: list()) -> binary().
translate(Lang, Text, Replace) ->
    iolist_to_binary(translate:translate(Lang, Text, Replace)).

get_host_server() ->
    case ejabberd_config:get_global_option(hosts) of
        [Host | _] ->
            Host;
        _ ->
            <<"localhost">>
    end.