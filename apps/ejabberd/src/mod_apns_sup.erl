%% ===================================================================
%%% @doc APNs supervisor
%%% @end
%% ===================================================================

-module(mod_apns_sup).

-behaviour(supervisor).

-include("mod_apns.hrl").

%% gen_mod callbacks
-export([start_link/0, start_connection/1, start_connection/2]).
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() ->
    {ok, pid()} | ignore | {error, {already_started, pid()} | shutdown | term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


-spec start_connection(apns:connection()) -> {ok, pid()} | {error, term()}.
start_connection(Connection) ->
    supervisor:start_child(?MODULE, [Connection]).


-spec start_connection(atom(), apns:connection()) ->
    {ok, pid()} | {error, term()}.
start_connection(Name, Connection) ->
    supervisor:start_child(?MODULE, [Name, Connection]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-spec init(_) ->
    {ok,
        {{simple_one_for_one, 5, 10},
            [{connection, {mod_apns_server, start_link, []},
                transient, 5000, worker, [apns_connection]}]}}.
init(_) ->
    {ok,
        {{simple_one_for_one, 5, 10},
            [{connection, {mod_apns_server, start_link, []},
                transient, 5000, worker, [apns_connection]}]}}.
