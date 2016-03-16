%%==============================================================================
%% Copyright China AFT by sharp.
%%==============================================================================
-module(mod_update).

%% cowboy_rest callbacks
-export([init/3,
    rest_init/2,
    rest_terminate/2]).

-export([allowed_methods/2,
    content_types_provided/2]).

-export([to_json/2]).

-record(state, {handler, opts, bindings}).

%%--------------------------------------------------------------------
%% cowboy_rest callbacks
%%--------------------------------------------------------------------
init({_Transport, http}, Req, Opts) ->
    {upgrade, protocol, cowboy_rest, Req, Opts}.

rest_init(Req, _Opts) ->
    {ok, Req, #state{}}.

rest_terminate(_Req, _State) ->
     ok.

allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

content_types_provided(Req, State) ->
    CTP = [{{<<"application">>, <<"json">>, '*'}, to_json}],
    {CTP, Req, State}.


%%--------------------------------------------------------------------
%% content_types_provided/2 callbacks
%%--------------------------------------------------------------------
to_json(Req, State) ->
    handle_get(mongoose_api_json, Req, State).


%%--------------------------------------------------------------------
%% HTTP verbs handlers
%%--------------------------------------------------------------------
handle_get(_Serializer, Req, State) ->
    Valid = parse_url(Req),
    case Valid of
        false -> error_response(bad_request, Req, State);
        {Server, Type} ->
            Result = get_update(Server, Type),
            {Result, Req, State}
    end.

%%--------------------------------------------------------------------
%% Error responses
%%--------------------------------------------------------------------
error_response(Code, Req, State) when is_integer(Code) ->
    {ok, Req1} = cowboy_req:reply(Code, Req),
    {halt, Req1, State};
error_response(Reason, Req, State) ->
    error_response(error_code(Reason), Req, State).

error_code(bad_request)     -> 400;
error_code(not_found)       -> 404;
error_code(conflict)        -> 409;
error_code(unprocessable)   -> 422;
error_code(not_implemented) -> 501.


parse_url(Req) ->
    { Varlist, _ } = cowboy_req:qs_vals( Req ),
    case length( Varlist ) of
        2 ->
            Server = keyfind(<<"server">>, Varlist),
            Type = keyfind(<<"type">>, Varlist),
            if
                ((Type =:= <<"0">>) or (Type =:= <<"1">>)) and (Server /= false) -> {Server, Type};
                true -> false
            end;
        _ ->
            false
    end.

get_update(LServer, Type) ->
    Query = ["select type, version_code, version_name, created_at, url, description from version_update where type='", Type , "' order by id desc limit 1;" ],

    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{Type, Code, Name, CreatedAt, Url, Description}]} ->
            F = mochijson2:encoder([{utf8, true}]),
            Json = {struct, [{<<"type">>, Type}, {<<"version_code">>, Code}, {<<"version_name">>, Name}, {<<"create_time">>, CreatedAt},
                {<<"url">>, Url}, {<<"description">>, Description}]},
            iolist_to_binary( F(Json) );
                _ ->
            <<>>
    end.

keyfind(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        false -> false;
        {_, Value} -> Value
    end.
