%%%===================================================================
%%% @doc push service hook
%%%
%%%===================================================================

-module(mod_push_hook).

-behaviour(gen_mod).

%% gen_mod callbacks
-export([start/2, stop/1]).

%% hook handlers
-export([push_service_hook/3]).

-include("mod_push_service.hrl").

%%%===================================================================
%%% gen_mod callbacks
%%%===================================================================
start(_Host, _Opts) ->
    ejabberd_hooks:add(push_service_hook, global, ?MODULE, push_service_hook, 50).

stop(_Host) ->
    ejabberd_hooks:delete(push_service_hook, global, ?MODULE, push_service_hook, 50).



%%%===================================================================
%%% hook
%%%===================================================================

-spec push_service_hook(binary(), binary(), binary()) -> ok | none.
push_service_hook(LServer, ToJid, PushContent) ->
    case odbc_push_service:get_tokens_by_jid(LServer, ToJid) of
        {ok, [_ | _] = PushTokenList} ->
            lists:foreach(fun(PushToken) ->
                push_notification(PushToken, PushContent)
            end, PushTokenList);
        _ ->
            none
    end.

-spec push_notification(#push_token{}, Content :: binary()) -> {ok | not_implement}.
push_notification(#push_token{type = PushType, token = DeviceToken}, Content) ->
    case PushType of
        <<"1">> -> %% APNS push
            mod_push_service_ios:send(DeviceToken, Content);
        _ -> %% Android or other
            not_implemented
    end.
