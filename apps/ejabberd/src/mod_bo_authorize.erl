%% ====================================================================================
%% generate authorize token for bo site
%% doc here: https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#bo-authorize
%%
%% ====================================================================================

-module(mod_bo_authorize).

-behaviour(gen_mod).

-export([start/2, stop/1, process_iq/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(NS_BO_AUTHORIZE, <<"aft:bo_authorize">>).

-record(token, {
    token :: binary(),
    expired :: integer()
}).

start(Host, _Opts) ->
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host,
        ?NS_BO_AUTHORIZE, ?MODULE, process_iq, no_queue).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_BO_AUTHORIZE).

%% ================================================
%% iq handler
%% ================================================

process_iq(From, To, #iq{xmlns = ?NS_BO_AUTHORIZE, type = _Type, sub_el = SubEl} = IQ) ->
    case SubEl of
        #xmlel{name = <<"query">>} ->
            case xml:get_tag_attr_s(<<"query_type">>, SubEl) of
                <<"get">> ->
                    get_token(From, To, IQ);
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
            end;
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;

process_iq(_, _, IQ) ->
    #iq{sub_el = SubEl} = IQ,
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.


get_token(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    Expire = integer_to_binary(generate_expiration()),
    S = list_to_binary(mod_mms_s3:secret()), %% use the same secret as mms
    Token = list_to_binary(generate_token(UserJid, Expire, S)),
    io:format("~p~n", [Token]),
    Res = mochijson2:encode({struct, record_to_json(#token{token = Token, expired = Expire})}),
    io:format("~p~n",[Res]),
    IQ#iq{type = result, sub_el = [SubEl#xmlel{children = [{xmlcdata, iolist_to_binary(Res)}]}]}.


%% ============================
%% helper
%% ============================

-spec generate_token(binary(), binary(), binary()) -> string().
generate_token(Jid, Expiration, Secret) ->
    md5(<<Jid/binary, Expiration/binary, Secret/binary>>).

-spec generate_expiration() -> integer().
generate_expiration() ->
    stamp_now() + 30 * 60. % 30 minutes expiration for authorize token

stamp_now() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.

-spec md5(string() | binary()) -> string().
md5(Text) ->
    lists:flatten([io_lib:format("~2.16.0b", [B]) || <<B>> <= erlang:md5(Text)]).

record_to_json(Token) ->
    [_ | L] = tuple_to_list(Token),
    lists:filter(fun(X) ->
        case X of
            {_, undefined} -> false;
            {_, null} -> false;
            _ -> true
        end
    end,
        lists:zip(record_info(fields, token), L)
    ).