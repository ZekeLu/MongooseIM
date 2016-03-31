%%==============================================================================
%% Copyright China AFT by sharp.
%%==============================================================================
-module(mod_update).

%% cowboy_rest callbacks
-export([init/3,
    rest_init/2,
    rest_terminate/2]).

-export([allowed_methods/2,
    content_types_provided/2,
    content_types_accepted/2]).

-export([to_json/2, from_json/2]).

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
    {[<<"GET">>, <<"POST">>], Req, State}.

content_types_provided(Req, State) ->
    CTP = [{{<<"application">>, <<"json">>, '*'}, to_json}],
    {CTP, Req, State}.

content_types_accepted(Req, State) ->
    CTA = [{{<<"application">>, <<"json">>, '*'}, from_json}],
    {CTA, Req, State}.


%%--------------------------------------------------------------------
%% content_types_provided/2 callbacks
%%--------------------------------------------------------------------
to_json(Req, State) ->
    handle_get(mongoose_api_json, Req, State).

%%--------------------------------------------------------------------
%% content_types_accepted/2 callbacks
%%--------------------------------------------------------------------
from_json(Req, State) ->
    handle_unsafe(mongoose_api_json, Req, State).


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

handle_unsafe(_Deserializer, Req, State) ->
    case cowboy_req:has_body(Req) of
        true ->
            {ok, Body, Req2} = cowboy_req:body(Req, [{length, 2000}]),
            {struct, Data} = mochijson2:decode(Body),
            {_, Signature} = lists:keyfind(<<"signature">>, 1, Data),
            {_, Type} = lists:keyfind(<<"type">>, 1, Data),
            {_, VersionCode} = lists:keyfind(<<"version_code">>, 1, Data),
            {_, VersionName} = lists:keyfind(<<"version_name">>, 1, Data),
            {_, Description} = lists:keyfind(<<"description">>, 1, Data),
            {_, Timestamp} = lists:keyfind(<<"timestamp">>, 1, Data),
            SignatureData = base64:decode(Signature),
            case verify_sign(SignatureData, Type, VersionCode, VersionName, Description, Timestamp) of
                true ->
                    %% TOFIX: incomplete.
                    %% 1. create RSA key. public key keep in server. private key keep in uploader.
                    %% 2. create table store SignData.
                    %% 3. check SignData is exist or not, if exist is replay attack, other is ok.
                    imcomplete;
                false ->
                    error_response(bad_request, Req, State)
            end;
        false ->
            error_response(bad_request, Req, State)
    end.

verify_sign(SignatureData, Type, VersionCode, VersionName, Description, Timestamp) ->
    Data = <<Type/binary, VersionCode/binary, VersionName/binary, Description/binary, Timestamp/binary>>,
    {ok, PublicKeyPath} = application:get_env(ejabberd, update_public_key),
    {ok, Content} = file:read_file(PublicKeyPath),
    [RSAEntry] = public_key:pem_decode(Content),
    PublicKey = public_key:pem_entry_decode(RSAEntry),
    public_key:verify(Data, sha, SignatureData, PublicKey).

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
            CreatedAtUTC = jlib:format_to_utc(CreatedAt),
            F = mochijson2:encoder([{utf8, true}]),
            Json = {struct, [{<<"type">>, Type}, {<<"version_code">>, Code}, {<<"version_name">>, Name}, {<<"create_time">>, CreatedAtUTC},
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
