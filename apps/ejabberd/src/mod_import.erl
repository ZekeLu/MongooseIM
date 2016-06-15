%%==============================================================================
%% Copyright China AFT by sharp.
%%==============================================================================
-module(mod_import).

%% cowboy_rest callbacks
-export([init/3,
    rest_init/2,
    rest_terminate/2]).

-export([allowed_methods/2, content_types_provided/2,
    content_types_accepted/2]).

-export([to_json/2, from_json/2]).

-record(state, {handler, opts, bindings}).

-record(project, {
    name,
    admin,
    photo,
    work_url,
    city,
    background
}).

-record(job, {
    name :: string(),
    lft :: integer(),
    rgt :: integer(),
    depth :: integer(),
    project :: integer(),
    department :: string(),
    department_level :: integer(),
    department_id :: integer(),
    users :: [string()]
}).


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
    handle_post(mongoose_api_json, Req, State).
%%--------------------------------------------------------------------
%% HTTP verbs handlers
%%--------------------------------------------------------------------

handle_get(_Deserializer, Req, State) ->
    handle_post(_Deserializer, Req, State).

handle_post(_Deserializer, Req, State) ->
    {Server, Req1} = cowboy_req:host(Req),
    {ok, Secret} = application:get_env(ejabberd, upload_secret),
    Token = list_to_binary(Secret),
    case cowboy_req:parse_header(<<"token">>, Req1) of
        {_, Token, Req2} ->
            {ok, Body, Req3} = cowboy_req:body(Req2),
            Data = jsx:decode(Body),
            case parse_data(Server, Data) of
                {ok, ProjectId} ->
                    Req4 = cowboy_req:reply(<<"200">>, [{<<"content-type">>, <<"text/plain">>}],
                        <<"done! project_id:", ProjectId/binady>>, Req3),
                    {halt, Req4, State};
                R ->
                    Req4 = cowboy_req:reply(<<"200">>, [{<<"content-type">>, <<"text/plain">>}],
                        term_to_binary(R), Req3),
                    {halt, Req4, State}
            end;
        _R ->
            Req2 = cowboy_req:reply(<<"400">>, [{<<"content-type">>, <<"text/plain">>}], <<"not valid">>, Req1),
            {halt, Req2, State}
    end.

mock_data() ->
    [
        {<<"project">>,
            [
                {<<"name">>, <<"test_project">>},
                {<<"admin">>, <<"+861340000000">>},
                {<<"city">>, <<"111111">>},
                {<<"background">>, <<"adfadfadfdadfadsfadfadf">>},
                {<<"work_url">>, <<"adfasdfadfadfadfadfadfadfadfa">>}
            ]
        },
        {<<"jobs">>,
            [
                [
                    {<<"name">>, <<"test_job1">>},
                    {<<"lft">>, 1},
                    {<<"rgt">>, 16},
                    {<<"depth">>, 1},
                    {<<"department">>, <<"depart_name">>},
                    {<<"department_level">>, 1},
                    {<<"department_id">>, 1},
                    {<<"users">>, [<<"+8613411111111">>, <<"+8613422222222">>]}
                ],
                [
                    {<<"name">>, <<"test_job2">>},
                    {<<"lft">>, 2},
                    {<<"rgt">>, 15},
                    {<<"depth">>, 1},
                    {<<"department">>, <<"depart_name2">>},
                    {<<"department_level">>, 2},
                    {<<"department_id">>, 2},
                    {<<"users">>, [<<"+8613433333333">>, <<"+8613444444444">>]}
                ]
            ]
        }
    ].

proplist_to_project(Props) ->
    P = proplists:get_value(<<"project">>, Props),
    List = lists:map(fun(X) ->
        proplists:get_value(atom_to_binary(X, utf8), P)
    end, record_info(fields, project)),
    L = [project | List],
    list_to_tuple(L).

proplist_to_job(Props) ->
    List = lists:map(fun(X) ->
        proplists:get_value(atom_to_binary(X, utf8), Props)
    end, record_info(fields, job)),
    L = [job | List],
    list_to_tuple(L).


parse_data(Server, D) ->
    case create_project(Server, proplist_to_project(D)) of
        {ok, ProjectId} ->
            R = lists:map(fun(X) ->
                case create_job(Server, proplist_to_job(X), ProjectId) of
                    ok ->
                        ok;
                    _ ->
                        failed
                end
            end, proplists:get_value(<<"jobs">>, D)),
            case lists:member(ok, R) of
                true ->
                    {ok, ProjectId};
                _ ->
                    {error, create_job_failed}
            end;
        _R ->
            {error, _R}
    end.


create_project(Server, Project) ->
    case create_user(Server, Project#project.admin) of
        {ok, Jid} ->
            Query = [<<"insert into project(name,admin,photo,work_url,background,city) values('">>,
                Project#project.name, <<"','">>, <<Jid/binary, $@, Server/binary>>, <<"','','">>,
                Project#project.work_url, <<"','">>,
                Project#project.background, <<"','">>, Project#project.city, <<"');">>],
            F = fun() ->
                ejabberd_odbc:sql_query_t(Query),
                ejabberd_odbc:sql_query_t(<<"select LAST_INSERT_ID();">>)
            end,
            case ejabberd_odbc:sql_transaction(Server, F) of
                {atomic, {selected, _, [{R}]}} ->
                    {ok, R};
                _Reason ->
                    {error, _Reason}
            end;
        _R ->
            {error, {create_user, _R}}
    end.


create_user(Server, Phone) ->
    Query = [<<"select username from users where cellphone ='">>, Phone, <<"';">>],
    case ejabberd_odbc:sql_query(Server, Query) of
        {selected, _, []} ->
            Jid = jlib:generate_uuid(),
            case ejabberd_auth:aft_try_register(Jid, Server, Phone, Phone) of
                {atomic, ok} ->
                    <<_:8/binary, Pass/binary>> = Phone,
                    case ejabberd_auth:set_password(Jid, Server, Pass) of
                        ok ->
                            {ok, Jid};
                        _R ->
                            {error, _R}
                    end;
                _Reason ->
                    {error, _Reason}
            end;
        {selected, _, [{R}]} ->
            {ok, R}
    end.


create_job(Server, Job, ProjectId) ->
    Users = lists:map(fun(X) ->
        case create_user(Server, X) of
            {ok, Jid} ->
                Jid;
            _ ->
                undefined
        end
    end, Job#job.users),
    case lists:member(undefined, Users) of
        true ->
            {error, create_user_failed};
        _ ->
            Query = [<<"insert into organization(name,lft,rgt,depth,department,project,department_level,department_id) values('">>,
                Job#job.name, <<"',">>, Job#job.lft, <<",">>, Job#job.rgt, <<",">>, Job#job.depth, <<",'">>,
                Job#job.department, <<"',">>, ProjectId, <<",">>, Job#job.department_level, <<",">>,
                Job#job.department_id, <<");">>],
            F = fun() ->
                {updated, 1} = ejabberd_odbc:sql_query_t(Query),
                {selected, _, [{Id}]} = ejabberd_odbc:sql_query_t(<<"select LAST_INSERT_ID();">>),
                lists:foreach(fun(X) ->
                    Query2 = [<<"insert into organization_user(organization,jid) values(">>, Id, <<",'">>,
                        <<X/binary, $@, Server/binary>>, <<"');">>],
                    ejabberd_odbc:sql_query_t(Query2)
                end, Users),
                ok
            end,
            case ejabberd_odbc:sql_transaction(Server, F) of
                {atomic, ok} ->
                    ok;
                _R ->
                    lager:error(">>>create failed: ~p~n", [_R]),
                    {error, _R}
            end
    end.
