%%%===================================================================
%%% @doc A module to filter user sent messages
%%%      Here are what it does to an AFT message:
%%%         1. remove the <sender/> element if the message includes one
%%%         2. add timestamp to the message
%%%===================================================================

-module(mod_aft_message).

-behaviour(gen_mod).

%% gen_mod callbacks
-export([start/2, stop/1]).

%% hook handlers
-export([filter_user_send_message/1]).

-include("jlib.hrl").

%%%===================================================================
%%% gen_mod callbacks
%%%===================================================================
start(_Host, _Opts) ->
    ejabberd_hooks:add(filter_user_send_message, global, ?MODULE, filter_user_send_message, 50).

stop(_Host) ->
    ejabberd_hooks:delete(filter_user_send_message, global, ?MODULE, filter_user_send_message, 50).

%%%===================================================================
%%% hook handlers
%%%===================================================================
filter_user_send_message(#xmlel{name = <<"message">>, children = Children} = XmlEl) ->
    case xml:get_subtag(XmlEl, <<"info">>) of
        false -> XmlEl;
        InfoEl ->
            #xmlel{children = InfoChildren} = InfoEl,
            %% the client should not include the <sender/> element
            %% if it does, we should remove it
            InfoEl1 = InfoEl#xmlel{children = lists:keydelete(<<"sender">>, #xmlel.name, InfoChildren)},

            {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
            TimeStamp = list_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ",
                [Year, Month, Day, Hour, Minute, Second])),
            %% and we will add timestamp to a message
            %% if the client specify the timestamp, we will remove it first
            InfoEl2 = xml:replace_tag_attr(<<"timestamp">>, TimeStamp, InfoEl1),

            OtherChildren = lists:keydelete(<<"info">>, #xmlel.name, Children),
            XmlEl#xmlel{children = [InfoEl2 | OtherChildren]}
    end;
filter_user_send_message(XmlEl) ->
    XmlEl.