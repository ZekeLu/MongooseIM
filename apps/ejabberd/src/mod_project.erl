-module(mod_project).

-behaviour(gen_mod).

%% API
-export([start/2, stop/1, process_iq/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("organization.hrl").

-define(TASK_PAGE_ITEM_COUNT, <<"10">>).


start(Host, _Opts) ->
    gen_iq_handler:add_iq_handler(ejabberd_local, Host, ?NS_AFT_PROJECT,
                                  ?MODULE, process_iq, no_queue),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_AFT_PROJECT,
                                  ?MODULE, process_iq, no_queue).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_local, Host, ?NS_AFT_PROJECT),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_AFT_PROJECT).

process_iq(From, To, #iq{xmlns = ?NS_AFT_PROJECT, sub_el = SubEl} = IQ) ->
    case xml:get_tag_attr_s(<<"type">>, SubEl) of
        <<"list_project">> ->
            list_project(From, To, IQ, list_project);
        <<"list_template">> ->
            list_project(From, To, IQ, list_template);
        <<"search_project">> ->
            search_project(From, To, IQ);
        <<"get_project">> ->
            get_project(From, To, IQ, self);
        <<"get_link_project">> ->
            get_project(From, To, IQ, link);
        <<"get_structure">> ->
            get_structure(From, To, IQ);
        <<"create">> ->
            create(From, To, IQ);
        <<"finish">> ->
            finish(From, To, IQ);
        <<"set_photo">> ->
            set_photo(From, To, IQ);
        <<"get_photo">> ->
            get_photo(From, To, IQ);
        <<"subscribe">> ->
            subscribe(From, To, IQ, subscribe);
        <<"subscribed">> ->
            subscribe(From, To, IQ, subscribed);
        <<"unsubscribed">> ->
            subscribe(From, To, IQ, unsubscribed);
        <<"unsubscribe">> ->
            subscribe(From, To, IQ, unsubscribe);
        <<"list_member">> ->
            list_member(From, To, IQ);
        <<"list_link_project">> ->
            list_link_project(From, To, IQ);
        <<"list_children_jobs">> ->
            list_children_jobs(From, To, IQ);
        <<"add_member">> ->
            add_member(From, To, IQ);
        <<"add_job">> ->
            add_job(From, To, IQ);
        <<"delete_member">> ->
            delete_member(From, To, IQ);
        <<"project_name_exist">> ->
            is_project_name_exist(From, To, IQ);
        <<"change_admin">> ->
            change_admin(From, To, IQ);
        <<"get_task_member">> ->
            get_task_member(From, To, IQ);
        <<"get_task">> ->
            get_task(From, To, IQ);
        _ ->
            IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}
    end;
process_iq(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


%% ------------------------------------------------------------------
%% higher function. called by process_iq.
%% ------------------------------------------------------------------

change_task_owner(LServer, Project, JID, GroupID, NewOwner, NickName) ->
    Query = ["select owner from groupinfo where groupid='", GroupID, "' and project='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{JID}]} ->
            F = fun() ->
                ejabberd_odbc:sql_query(LServer, "update groupinfo set owner='", ejabberd_odbc:escape(NewOwner), "' where groupid='", GroupID, "';"),
                ejabberd_odbc:sql_query(LServer, "insert into groupuser(groupid, jid, nickname) values('", GroupID, "', '",
                    ejabberd_odbc:escape(JID), "', '", ejabberd_odbc:escape(NickName), "');"),
                ok
            end,
            case ejabberd_odbc:escape(LServer, F) of
                {atomic, ok} -> ok;
                _ -> error
            end;
        _ ->
            error
    end.


get_project(From, _To, #iq{type = get, sub_el = SubEl} = IQ, Who) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Project} = lists:keyfind(<<"project">>, 1, Data),
    ProjectTarget =
        case Who of
            self -> false;
            link ->
                {_, Value} = lists:keyfind(<<"project_target">>, 1, Data),
                Value
        end,

    case get_project_ex(S, BaseJID, Project, ProjectTarget) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
get_project(_, _, IQ, _) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

get_structure(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    Project = case lists:keyfind(<<"project">>, 1, Data) of
                  false -> false;
                  {_, Value1} -> Value1
              end,
    Template = case lists:keyfind(<<"template">>, 1, Data) of
                   false -> false;
                   {_, Value2} -> Value2
               end,
    ProjectTarget = case lists:keyfind(<<"project_target">>, 1, Data) of
                        false -> false;
                        {_, Value3} -> Value3
                    end,

    case get_structure_ex(S, BaseJID, Project, ProjectTarget, Template) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
get_structure(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

create(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Name} = lists:keyfind(<<"name">>, 1, Data),
    {_, Template} = lists:keyfind(<<"template">>, 1, Data),
    {_, Job} = lists:keyfind(<<"job">>, 1, Data),

    case create_ex(S, Name, BaseJID, Template, Job) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
create(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

finish(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ProID} = lists:keyfind(<<"project">>, 1, Data),

    case finish_ex(S, ProID, BaseJID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, EndTime} ->
            %% TOFIX: finished all task and all event and all data r-s.
            case odbc_organization:get_all_jid(S, ProID) of
                {ok, Result} ->
                    Content = <<"{\"end_time\":\"", EndTime/binary, "\"}">>,
                    push_message(ProID, S, Result, <<"finished">>, Content);
                _ ->
                    %% how to fix: check finish status before operator???
                    ?ERROR_MSG("[Project:~p] push finish msg failed.", [ProID])
            end,
            IQ#iq{type = result}
    end;
finish(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

set_photo(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ProID} = lists:keyfind(<<"project">>, 1, Data),
    {_, Photo} = lists:keyfind(<<"photo">>, 1, Data),

    case set_photo_ex(S, ProID, BaseJID, Photo) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        ok ->
            F = mochijson2:encoder([{utf8, true}]),
            Content = iolist_to_binary(F({struct, [{<<"project">>, ProID}, {<<"photo">>, list_to_binary(make_head_url(binary_to_list(Photo)))}]})),
            {ok, Result} = odbc_organization:get_all_jid(S, ProID),
            push_message(ProID, S, Result, <<"set_photo">>, Content),
            IQ#iq{type = result}
    end;
set_photo(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

get_photo(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = _U, server = S} = From,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    ProID = case lists:keyfind(<<"project">>, 1, Data) of
                {_, Value1} -> Value1;
                false -> false
            end,
    TempID = case lists:keyfind(<<"template">>, 1, Data) of
                {_, Value2} -> Value2;
                false -> false
            end,

    case get_photo_ex(S, ProID, TempID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
get_photo(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

list_project(From, _To, #iq{type = get, sub_el = SubEl} = IQ, Type) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,

    case list_project_ex(S, BaseJID, Type) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
list_project(_, _, IQ, _Type) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

list_children_jobs(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ProID} = lists:keyfind(<<"project">>, 1, Data),
    {_, Job} = lists:keyfind(<<"job">>, 1, Data),

    case list_children_jobs_ex(S, BaseJID, ProID, Job) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
list_children_jobs(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

add_job(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ProID} = lists:keyfind(<<"project">>, 1, Data),
    {_, ParentJobID} = lists:keyfind(<<"parent_job_id">>, 1, Data),
    {_, JobName} = lists:keyfind(<<"job_name">>, 1, Data),
    {_, Part} = lists:keyfind(<<"part">>, 1, Data),

    case add_job_ex(S, ProID, BaseJID, ParentJobID, JobName, Part) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result, JobTag} ->
            {ok, AllMembers} = odbc_organization:get_all_jid(S, ProID),
            Content = <<"{\"job_tag\":\"", JobTag/binary, "\"}">>,
            push_message(ProID, S, AllMembers, <<"add_job">>, Content),
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
add_job(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

add_member(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ProID} = lists:keyfind(<<"project">>, 1, Data),
    {_, Members} = lists:keyfind(<<"member">>, 1, Data),

    if length(Members) > 0 ->
        Ls = parse_json_list([<<"job_id">>, <<"jid">>], Members, []),
        case add_member_ex(S, ProID, BaseJID, Ls) of
            {error, Error} ->
                IQ#iq{type = error, sub_el = [SubEl, Error]};
            {ok, MemberTag, ValidMembers} ->
                Content1 = [{struct, [{<<"job_id">>, V1}, {<<"jid">>, V2}]} || {V1, V2} <- ValidMembers],
                F = mochijson2:encoder([{utf8, true}]),
                Content = iolist_to_binary(F({struct, [{<<"project">>, ProID}, {<<"member_tag">>, MemberTag}, {<<"member">>, Content1}]})) ,
                case odbc_organization:get_all_jid(S, ProID) of
                    {ok, Result} ->
                        push_message(ProID, S, Result, <<"add_member">>, Content),
                        IQ#iq{type = result};
                    _ ->
                        IQ#iq{type = error, sub_el = [SubEl, ?AFT_ERR_DATABASE]}
                end
        end;
       true ->
            IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}
    end;
add_member(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

delete_member(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ProID} = lists:keyfind(<<"project">>, 1, Data),
    {_, SelfJob} = lists:keyfind(<<"self_job_id">>, 1, Data),
    {_, JID} = lists:keyfind(<<"jid">>, 1, Data),
    {_, Job} = lists:keyfind(<<"job">>, 1, Data),
    case delete_member_ex(S, ProID, BaseJID, SelfJob, JID, Job) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, MemberTag} ->
            case odbc_organization:get_all_jid(S, ProID) of
                {ok, Result} ->
                    Result0 = [{JID} | Result], %% include delete member.
                    Content = <<"{\"project\":\"", ProID/binary,  "\", \"member_tag\":\"", MemberTag/binary,  "\", \"member\":[\"", JID/binary, "\"]}">>,
                    push_message(ProID, S, Result0, <<"delete_member">>, Content),
                    ejabberd_hooks:run(delete_member, jlib:nameprep(S), [S, JID, ProID]),
                    IQ#iq{type = result};
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?AFT_ERR_DATABASE]}
            end
    end;
delete_member(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

list_member(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),

    {_, Project} = lists:keyfind(<<"project">>, 1, Data),
    ProjectTarget = case lists:keyfind(<<"project_target">>, 1, Data) of
                        false -> false;
                        {_, Target} -> Target
                    end,

    case list_member_ex(S, Project, BaseJID, ProjectTarget) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
list_member(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

list_link_project(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Project} = lists:keyfind(<<"project">>, 1, Data),

    case list_link_project_ex(S, Project, BaseJID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
list_link_project(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

subscribe(From, _To, #iq{type = set, sub_el = SubEl} = IQ, Type) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, IDSelf} = lists:keyfind(<<"id_self">>, 1, Data),
    {_, IDTarget} = lists:keyfind(<<"id_target">>, 1, Data),

    case subscribe_ex(S, BaseJID, IDSelf, IDTarget, Type) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        ok ->
            {ok, [{ProNameSelf}]} = odbc_organization:project_name(S, IDSelf),
            {ok, [{ProNameTarget}]} = odbc_organization:project_name(S, IDTarget),
            Content = <<"{\"id_self\":\"", IDTarget/binary, "\", \"name_self\":\"", ProNameTarget/binary,
                        "\", \"id_target\":\"", IDSelf/binary, "\", \"name_target\":\"", ProNameSelf/binary, "\"}">>,
            case get_admin(S, IDTarget) of
                {true, Admin} ->
                    push_message(IDSelf, S, [{Admin}], atom_to_binary(Type, latin1), Content);
                _ ->
                    ?ERROR_MSG("[Project:~p] has no admin.", [IDTarget])
            end,
            IQ#iq{type = result};
        {ok, LinkTag} ->
            {ok, [{ProNameSelf}]} = odbc_organization:project_name(S, IDSelf),
            {ok, [{ProNameTarget}]} = odbc_organization:project_name(S, IDTarget),
            {ok, IDSelf_Member} = odbc_organization:get_all_jid(S, IDSelf),
            {ok, IDTarget_Member} = odbc_organization:get_all_jid(S, IDTarget),
            Content1 = <<"{\"id_self\":\"", IDSelf/binary, "\", \"name_self\":\"", ProNameSelf/binary, "\", \"link_tag\":\"", LinkTag/binary,
                         "\", \"id_target\":\"", IDTarget/binary, "\", \"name_target\":\"", ProNameTarget/binary, "\"}">>,
            Content2 = <<"{\"id_self\":\"", IDTarget/binary, "\", \"name\":\"", ProNameTarget/binary, "\", \"link_tag\":\"", LinkTag/binary,
                         "\", \"id_target\":\"", IDSelf/binary, "\", \"name_target\":\"", ProNameSelf/binary, "\"}">>,
            push_message(IDSelf, S, IDSelf_Member, atom_to_binary(Type, latin1), Content1),
            push_message(IDTarget, S, IDTarget_Member, atom_to_binary(Type, latin1), Content2),
            IQ#iq{type = result}
    end;
subscribe(_, _, IQ, _) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

search_project(From, _To, #iq{sub_el = SubEl} = IQ) ->
    #jid{server = S} = From,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Name} = lists:keyfind(<<"name">>, 1, Data),

    case search_project_ex(S, Name) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
search_project(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

%% current not used.
is_project_name_exist(From, _To, #iq{sub_el = SubEl} = IQ) ->
    #jid{user = _U, server = S} = From,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Name} = lists:keyfind(<<"name">>, 1, Data),
    case odbc_organization:is_project_name_exist(S, Name) of
        false ->
            IQ#iq{type = result, sub_el = [SubEl]};
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?AFT_ERR_PROJECT_NAME_EXIST]}
    end;
is_project_name_exist(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

change_admin(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Project} = lists:keyfind(<<"project">>, 1, Data),
    {_, Admin} = lists:keyfind(<<"admin">>, 1, Data),

    case change_admin_ex(S, Project, Admin, BaseJID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        ok ->
            IQ#iq{type = result}
    end;
change_admin(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

get_task_member(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Project} = lists:keyfind(<<"project">>, 1, Data),
    {_, JobID} = lists:keyfind(<<"job_id">>, 1, Data),

    case get_task_member_ex(S, Project, BareJID, JobID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
get_task_member(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


get_task(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Project} = lists:keyfind(<<"project">>, 1, Data),
    {_, SelfJobID} = lists:keyfind(<<"self_job_id">>, 1, Data),
    {_, TargetJobID} = lists:keyfind(<<"job_id">>, 1, Data),
    {_, JID} = lists:keyfind(<<"jid">>, 1, Data),
    {_, Page} = lists:keyfind(<<"page">>, 1, Data),

    case get_task_ex(S, Project, BareJID, SelfJobID, JID, TargetJobID, Page) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
get_task(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.




%% ------------------------------------------------------------------
%% lower function. called by higher function, bridge between odbc and higher function.
%% ------------------------------------------------------------------

get_project_ex(LServer, BaseJID, ProID, ProjectTarget) ->
    Valid =
        case ProjectTarget of
            false ->
                case odbc_organization:is_memeber(LServer, ProID, BaseJID) of
                    true -> {true, self};
                    _ -> {is_predefine_template(LServer, ProID), self}
                end;
            _ ->
                case odbc_organization:is_link_member(LServer, ProID, BaseJID, ProjectTarget) of
                    true -> {true, link};
                    _ ->false
                end
        end,
    case Valid of
        {true, self} ->
            {ok, Result} = odbc_organization:get_project(LServer, ProID),
            F = mochijson2:encoder([{utf8, true}]),
            Json1 = [{struct, [{"id", R1}, {"name", R2}, {"description", R3}, {"photo", list_to_binary(make_head_url(binary_to_list(R4)))},
                               {"status", R5}, {"admin", R6}, {"start_time", R7}, {"end_time", R8}, {"job_tag", R9},
                               {"member_tag", R10}, {"link_tag", R11}]}
                     || {R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11} <- Result ],
            Json = iolist_to_binary( F( Json1 ) ),
            {ok, Json};
        {true, link} ->
            {ok, R1, R2, _R3, R4, _R5, _R6, _R7, _R8, R9, R10, _R11} = odbc_organization:get_project(LServer, ProjectTarget),
            F = mochijson2:encoder([{utf8, true}]),
            Json = {struct, [{"id", R1}, {"name", R2}, {"photo", list_to_binary(make_head_url(binary_to_list(R4)))},
                {"job_tag", R9}, {"member_tag", R10}]},
            {ok, iolist_to_binary(F({struct, [{<<"self_project">>, ProID}, {<<"link_project">>, Json}]}))};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

list_project_ex(LServer, BaseJID, Type) ->
    case Type of
        list_project ->
            case odbc_organization:list_project(LServer, BaseJID, false) of
                {ok, Result} ->
                    F = mochijson2:encoder([{utf8, true}]),
                    Json1 = [{struct, [{"id", R1}, {"name", R2}, {"description", R3}, {"photo",photo_url(R4)},
                        {"status", R5}, {"admin", R6}, {"start_time", R7}, {"end_time", R8},
                        {"job_tag", R9}, {"member_tag", R10}, {"link_tag", R11}]}
                        || {R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11} <- Result ],
                    Json = iolist_to_binary( F( Json1 ) ),
                    {ok, Json};
                {error, _Reason} ->
                    {error, ?AFT_ERR_DATABASE}
            end;
        list_template ->
            case odbc_organization:list_project(LServer, BaseJID, true) of
                {ok, Result} ->
                    F = mochijson2:encoder([{utf8, true}]),
                    Json1 = [{struct, [{"id", R1}, {"name", R2}, {"description", R3},
                        {"photo", photo_url(R4)}, {"job_tag", R5}]}
                        || {R1, R2, R3, R4, R5} <- Result ],
                    Json = iolist_to_binary( F( Json1 ) ),
                    {ok, Json};
                {error, _Reason} ->
                    {error, ?AFT_ERR_DATABASE}
            end
    end.

%% remove check authority???  %% (LServer, ProjectSelf, Jid, ProjectTarget)
get_structure_ex(LServer, BaseJID, ProID, ProjectTarget, Template) ->
    Valid = case Template of
                false ->
                    case odbc_organization:is_memeber(LServer, ProID, BaseJID) of
                        true ->
                            case ProjectTarget of
                                false ->
                                    {true, ProID};
                                _ ->
                                    case odbc_organization:is_link_member(LServer, ProID, BaseJID, ProjectTarget) of
                                        true -> {true, ProjectTarget};
                                        _ ->{true, ProID}
                                    end
                            end;
                        _ -> {false, ProID}
                    end;
                _ ->
                    {is_predefine_template(LServer, Template), Template}
            end,
    case Valid of
        {true, Project} ->
            {ok, Result} = odbc_organization:get_structure(LServer, Project),
            Json1 = [{struct,[{<<"id">>, R1}, {<<"name">>, R2}, {<<"left">>, R3}, {<<"right">>, R4}, {<<"part">>, R5}]}
                     || {R1, R2, R3, R4, R5}<- Result],
            F = mochijson2:encoder([{utf8, true}]),
            Json = iolist_to_binary( F( {struct, [{<<"project">>, Project}, {<<"structure">>, Json1}]} )),
            {ok, Json};
        {false, _} ->
            {error, ?AFT_ERR_INVALID_TEMPLATE}
    end.

create_ex(LServer, ProjectName, BaseJID, Template, Job) ->
    case odbc_organization:is_project_name_exist(LServer, ProjectName) of
        false ->
            case odbc_organization:node_exist(LServer, Template, Job) of
                {ok, true} ->
                    case odbc_organization:add_project(LServer, #project{name = ProjectName, description = <<"">>, admin = BaseJID}, Template, Job) of
                        {ok, #project{id = Id, name = _Name, photo = Photo ,description = _Desc, job_tag = JobTag, start_at = StartTime},
                         #node{id = JobId, name=JobName, lft = Left, rgt = Right, department = Part}} ->
                            PhotoURL = list_to_binary(make_head_url(binary_to_list(Photo))),
                            ejabberd_hooks:run(create_project, LServer, [LServer, Id]),
                            {ok, <<"{\"project\":{\"id\":\"", Id/binary, "\",\"name\":\"", ProjectName/binary,
                                    "\",\"photo\":\"", PhotoURL/binary, "\",\"job_tag\":\"", JobTag/binary,
                                    "\",\"member_tag\":\"", JobTag/binary, "\",\"link_tag\":\"", JobTag/binary,
                                    "\",\"start_time\":\"", StartTime/binary, "\"},
                                    \"job\":{\"job_id\":\"", JobId/binary, "\",\"job_name\":\"", JobName/binary,
                                    "\",\"left\":\"", Left/binary, "\",\"right\":\"", Right/binary,
                                    "\",\"part\":\"", Part/binary, "\"},
                                    \"member\":{\"jid\":\"", BaseJID/binary, "\"}}">>};
                        {error, _} ->
                            ?ERROR_MSG("Create Project failed, ProjectName=~p.", [ProjectName]),
                            {error, ?AFT_ERR_DATABASE}
                    end;
                {ok, false} ->
                    {error, ?AFT_ERR_INVALID_JOB}
            end;
        _ ->
            {error, ?AFT_ERR_PROJECT_NAME_EXIST}
    end.

finish_ex(LServer, ProID, BaseJID) ->
    case is_admin(LServer, BaseJID, ProID) of
        true ->
            case odbc_organization:project_status(LServer, ProID) of
                {ok, [{<<"1">>}]} ->
                    case odbc_organization:finish_project(LServer, ProID) of
                        {ok, EndTime} -> {ok, EndTime};
                        {error, _Reason} -> {error, ?AFT_ERR_DATABASE}
                    end;
                {ok, [{<<"0">>}]} ->
                    {error, ?AFT_ERR_ALLREADY_FINISHED};
                {ok, _} ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        false ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

subscribe_status(true,  false, subscribe)    -> subscribe_again;
subscribe_status(false, true,  subscribe)    -> subscribed;
subscribe_status(false, false, subscribe)    -> subscribe;
subscribe_status(_,     false, subscribed)   -> {error, ?AFT_ERR_TARGET_NO_SUBSCRIBE_REQUEST};
subscribe_status(false, true,  subscribed)   -> subscribed;
subscribe_status(_,     false, unsubscribed) -> {error, ?AFT_ERR_TARGET_NO_SUBSCRIBE_REQUEST};
subscribe_status(false, true,  unsubscribed) -> unsubscribed;
subscribe_status(true,  true,  unsubscribe)  -> unsubscribe;
subscribe_status(_,     _,     unsubscribe)  -> {error, ?AFT_ERR_NO_SUBSCRIBED};
subscribe_status(true,  true,  _)            -> {error, ?AFT_ERR_ALLREADY_SUBSCRIBED}.

subscribe_ex(LServer, JID, ProSource, ProTarget, Type) ->
    case is_admin(LServer, JID, ProSource) of
        true ->
            case odbc_organization:is_project_exist(LServer, ProTarget) of
                true ->
                    To = odbc_organization:is_subscribe_exist(LServer, ProSource, ProTarget),
                    From = odbc_organization:is_subscribe_exist(LServer, ProTarget, ProSource),
                    case subscribe_status(To, From, Type) of
                        {error, Error} -> {error, Error};
                        subscribe ->
                            case odbc_organization:subscribe(LServer, ProSource, ProTarget) of
                                ok -> ok;
                                _ -> {error, ?AFT_ERR_DATABASE}
                            end;
                        subscribe_again ->
                            ok;
                        subscribed ->
                            case odbc_organization:subscribed(LServer, ProSource, ProTarget) of
                                {ok, LinkTag} -> {ok, LinkTag};
                                _ -> {error, ?AFT_ERR_DATABASE}
                            end;
                        unsubscribed ->
                            odbc_organization:unsubscribed(LServer, ProTarget, ProSource);
                        unsubscribe ->
                            odbc_organization:unsubscribe(LServer, ProSource, ProTarget)
                    end;
                _ ->
                    {error, ?AFT_ERR_PROJECT_NOT_EXIST}
            end;
        false ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

search_project_ex(LServer, Name) ->
    case odbc_organization:search_project(LServer, Name) of
        {ok, Result} ->
            %%{ok, build_json(["id", "name"], Result, true, <<>>)};
            Json = [{struct, [{<<"id">>, R1}, {<<"name">>, R2}]} || {R1, R2} <- Result],
            F = mochijson2:encoder([{utf8, true}]),
            {ok, iolist_to_binary(F(Json))};
        {error, _Reson} ->
            failed
    end.

list_children_jobs_ex(LServer, JID, ProID, Job) ->
    case is_admin(LServer, JID, ProID) of
        true ->
            {ok, Result} = odbc_organization:get_all_nodes(LServer, ProID),
            %%{ok, build_json(["job_id", "job_name", "part"], Result, true, ProID)};
            Json = [{struct, [{<<"job_id">>, R1}, {<<"job_name">>, R2}, {<<"part">>, R3}]}
                    || {R1, R2, R3} <- Result],
            F = mochijson2:encoder([{utf8, true}]),
            {ok, iolist_to_binary(F({struct, [{<<"project">>, ProID}, {<<"job">>, Json}]}))};
        false ->
            case odbc_organization:is_member2(LServer, ProID, Job, JID) of
                false ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH};
                true ->
                    {ok, Result} = odbc_organization:get_children_job(LServer, Job, ProID),
                    %{ok, build_json(["job_id", "job_name", "part"], Result, true, ProID)}
                    Json = [{struct, [{<<"job_id">>, R1}, {<<"job_name">>, R2}, {<<"part">>, R3}]}
                            || {R1, R2, R3} <- Result],
                    F = mochijson2:encoder([{utf8, true}]),
                    {ok, iolist_to_binary(F({struct, [{<<"project">>, ProID}, {<<"job">>, Json}]}))}
            end
    end.

check_add_member_valid(LServer, ProID, _BaseJID, List) ->
    %% TOFIX: check BaseJID is List jid job's parent in project ???
    %% only check BaseJID in project is ok.
    case odbc_organization:get_structure(LServer, ProID) of
        {ok, Result} ->
            {ValidJobList, _InvalidJobList} = lists:partition(
                                                fun({E_ID, _}) ->
                                                        case lists:keyfind(E_ID, 1, Result) of
                                                            false -> false;
                                                            _ -> true
                                                        end
                                                end,
                                                List),
            {ok, Result2} = odbc_organization:get_all(LServer, ProID),
            ExistList = lists:filtermap(
                          fun({E_JID, E_ID, _E_Name, _E_Part}) ->
                                  {true, {E_ID, E_JID}}
                          end,
                          Result2),
            {ValidList, _DuplicationList} = lists:partition(
                                              fun(E) ->
                                                      case lists:member(E, ExistList) of
                                                          false -> true;
                                                          _ -> false
                                                      end
                                              end,
                                              ValidJobList),
            ?ERROR_MSG("add_member invalid job list:~p~n,duplication list:~p",
                       [_InvalidJobList, _DuplicationList]),
            {ok, ValidList};
        _ ->
            {error, ?AFT_ERR_DATABASE}
    end.

add_member_ex(LServer, ProID, BaseJID, List) ->
    case odbc_organization:project_status(LServer, ProID) of
        {ok, [{<<"1">>}]} ->
            case odbc_organization:is_memeber(LServer, ProID, BaseJID) of
                true ->
                    case check_add_member_valid(LServer, ProID, BaseJID, List) of
                        {error, Error} ->
                            {error, Error};
                        {ok, ValidList} ->
                            if length(ValidList) > 0 ->
                                    case odbc_organization:add_employees(LServer, ProID, BaseJID, ValidList) of
                                        {ok, MemberTag} ->
                                            {ok, MemberTag, ValidList};
                                        {error, _Reason} ->
                                            {error, ?AFT_ERR_DATABASE}
                                    end;
                               true ->
                                    {error, ?AFT_ERR_MEMBER_INVALID}
                            end
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {ok, [{<<"0">>}]} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        {ok, _} ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

add_job_ex(LServer, ProID, BaseJID, ParentJobID, JobName, Part) ->
    case odbc_organization:project_status(LServer, ProID) of
        {ok, [{<<"1">>}]} ->
            case odbc_organization:get_job_info(LServer, ParentJobID) of
                {ok, []} ->
                    {error, ?AFT_ERR_PARENT_NOT_EXIST};
                {ok, [{_, _, _, _, _, ParentPart, _}]} ->
                    Valid = case is_admin(LServer, BaseJID, ProID) of
                                false ->
                                    odbc_organization:is_member2(LServer, ProID, ParentJobID, BaseJID);
                                true -> true
                            end,
                    case Valid of
                        false -> {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH};
                        true ->
                            if ParentPart =:= Part ->
                                case odbc_organization:add_node(LServer, ParentJobID, #node{name = JobName, department = Part}) of
                                    {ok, JobTag, #node{id = Id, lft = Left, rgt = Right}} ->
                                        {ok, build_json([{"project", ProID}, {"job", {["id", "name", "left", "right", "part"],
                                            [{Id, JobName, Left, Right, Part}], false}}], <<>>), JobTag};
                                    {error, _} ->
                                        {error, ?AFT_ERR_DATABASE}
                                end;
                                true ->
                                    case odbc_organization:get_department_member_parent(LServer, ProID, Part) of
                                        {ok, ParentJobList} ->
                                            case lists:member({ParentJobID}, ParentJobList) of
                                                true ->
                                                    case odbc_organization:add_node(LServer, ParentJobID, #node{name = JobName, department = Part}) of
                                                        {ok, JobTag, #node{id = Id, lft = Left, rgt = Right}} ->
                                                            {ok, build_json([{"project", ProID}, {"job", {["id", "name", "left", "right", "part"],
                                                                [Id, JobName, Left, Right, Part], false}}], <<>>), JobTag};
                                                        {error, _} ->
                                                            {error, ?AFT_ERR_DATABASE}
                                                    end;
                                                false ->
                                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                                            end
                                    end
                            end
                    end;
                _ ->
                    {error, ?AFT_ERR_DATABASE}
            end;
        {ok, [{<<"0">>}]} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        {ok, _} ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

list_link_project_ex(LServer, ProID, BaseJID) ->
    case odbc_organization:is_memeber(LServer, ProID, BaseJID) of
        true ->
            {ok, Result} = odbc_organization:get_link_project(LServer, ProID),
            %%{ok, build_json([ {"self_project", ProID}, {"link_project", {["id", "name"], Result, true}} ], <<>>)};
            Json = [{struct, [{<<"id">>, R1}, {<<"name">>, R2}, {<<"photo">>, list_to_binary(make_head_url(binary_to_list(R3)))},
                {<<"job_tag">>, R4}, {<<"member_tag">>, R5}]} || {R1, R2, R3, R4, R5} <- Result],
            F = mochijson2:encoder([{utf8, true}]),
            {ok, iolist_to_binary(F({struct, [{<<"self_project">>, ProID}, {<<"link_project">>, Json}]}))};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

list_member_ex(LServer, ProID, BaseJID, ProjectTarget) ->
    case odbc_organization:is_memeber(LServer, ProID, BaseJID) of
        true ->
            case ProjectTarget of
                false ->
                    {ok, Result} = odbc_organization:get_all(LServer, ProID),
                    %%{ok, build_json([ {"project", ProID}, {"member", {["jid", "job_id", "job_name", "part"], Result, true}} ], <<>>)};
                    Json = [{struct, [{<<"jid">>, R1}, {<<"job_id">>, R2}]}
                            || {R1, R2, _R3, _R4} <- Result],
                    F = mochijson2:encoder([{utf8, true}]),
                    {ok, iolist_to_binary(F({struct, [{<<"project">>, ProID}, {<<"member">>, Json}]}))};
                _ ->
                    case odbc_organization:is_link_member(LServer, ProID, BaseJID, ProjectTarget) of
                        true ->
                            {ok, Result} = odbc_organization:get_all(LServer, ProjectTarget),
                            %%{ok, build_json([ {"project", ProjectTarget}, {"member", {["jid", "job_id", "job_name", "part"], Result, true}} ], <<>>)};
                            Json = [{struct, [{<<"jid">>, R1}, {<<"job_id">>, R2}]}
                                    || {R1, R2, _R3, _R4} <- Result],
                            F = mochijson2:encoder([{utf8, true}]),
                            {ok, iolist_to_binary(F({struct, [{<<"project">>, ProjectTarget}, {<<"member">>, Json}]}))};
                        _ ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

delete_member_ex(LServer, ProID, BaseJID, SelfJob, JID, Job) ->
    case odbc_organization:project_status(LServer, ProID) of
        {ok, [{<<"1">>}]} ->
            case BaseJID =:= JID of
                true ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH};
                _ ->
                    PriviligeOK = case is_admin(LServer, BaseJID, ProID) of
                                      true -> ok;
                                      false ->
                                          case odbc_organization:is_child(LServer, ProID, BaseJID, SelfJob, JID, Job) of
                                              false -> {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH};
                                              true ->
                                                  case is_admin(LServer, JID, ProID) of
                                                      true ->
                                                          {error, ?AFT_ERR_DELETE_ADMIN};
                                                      false ->
                                                          ok
                                                  end
                                          end
                                  end,

                    case PriviligeOK of
                        ok ->
                            case odbc_organization:delete_task(LServer, ProID, JID) of
                                ok ->
                                    case odbc_organization:delete_employee(LServer, ProID, JID, Job) of
                                        {ok, MemberTag} ->
                                            {ok, MemberTag};
                                        {error, _Reason} ->
                                            {error, ?AFT_ERR_DATABASE}
                                    end;
                                {continue, _PairTask} -> %% should return this pair task to client???
                                    {error, ?AFT_ERR_JOIN_PAIR_TASK};
                                _ ->
                                    {error, ?AFT_ERR_DATABASE}
                            end;
                        Error ->
                            Error
                    end
            end;
        {ok, [{<<"0">>}]} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        {ok, _} ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

set_photo_ex(LServer, ProID, BaseJID, Photo) ->
    case is_admin(LServer, BaseJID, ProID) of
        true ->
            case odbc_organization:set_photo(LServer, ProID, Photo) of
                ok -> ok;
                _ -> {error, ?AFT_ERR_DATABASE}
            end;
        false ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

get_photo_ex(LServer, ProID, TempID) ->
    case TempID of
        false ->
            case odbc_organization:get_photo(LServer, ProID, false) of
                {ok, []} ->
                    {ok, <<"{\"project\":\"", ProID/binary, "\", \"photo\":\"\"}">>};
                {ok, [{Photo}]} ->
                    PhotoUrl = photo_url(Photo),
                    {ok, <<"{\"project\":\"", ProID/binary, "\", \"photo\":\"", PhotoUrl/binary, "\"}">>}
            end;
        _ ->
            case odbc_organization:get_photo(LServer, TempID, true) of
                {ok, []} ->
                    {ok, <<"{\"template\":\"", TempID/binary, "\", \"photo\":\"\"}">>};
                {ok, [{Photo}]} ->
                    PhotoUrl = photo_url(Photo),
                    {ok, <<"{\"template\":\"", TempID/binary, "\", \"photo\":\"", PhotoUrl/binary, "\"}">>}
            end
    end.

change_admin_ex(LServer, Project, Admin, BaseJID) ->
    case odbc_organization:is_memeber(LServer, Project, Admin) of
        true ->
            {true, OldAdmin} = get_admin(LServer, Project),
            if
                (Admin =:= OldAdmin) or (BaseJID =:= Admin) -> {error, ?AFT_ERR_MEMBER_INVALID};
                true ->
                    if
                        BaseJID =:= OldAdmin ->
                            case odbc_organization:change_admin(LServer, Project, Admin) of
                                ok -> ok;
                                _ -> {error, ?AFT_ERR_DATABASE}
                            end;
                        true ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
%%                             case odbc_organization:get_job(LServer, BaseJID, Project) of
%%                                 {ok, Job} ->
%%                                     {ok, OldAdminJob} = odbc_organization:get_job(LServer, OldAdmin, Project),
%%                                     if
%%                                         Job =:= OldAdminJob ->
%%                                             case odbc_organization:change_admin(LServer, Project, Admin) of
%%                                                 ok -> ok;
%%                                                 _ -> {error, ?AFT_ERR_DATABASE}
%%                                             end;
%%                                         true ->
%%                                             case odbc_organization:get_parent_jids(LServer, BaseJID, Project) of
%%                                                 {error, _} ->
%%                                                     {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH};
%%                                                 {ok, Parents} ->
%%                                                     ParentJIDs = [P_JID || #employee{jid = P_JID} <- Parents],
%%                                                     case lists:member(BaseJID, ParentJIDs) of
%%                                                         true ->
%%                                                             case odbc_organization:change_admin(LServer, Project, Admin) of
%%                                                                 ok -> ok;
%%                                                                 _ -> {error, ?AFT_ERR_DATABASE}
%%                                                             end;
%%                                                         false ->
%%                                                             {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
%%                                                     end
%%                                             end
%%                                     end;
%%                                 _ ->
%%                                     {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
%%                             end
                    end
            end
    end.

get_task_member_ex(LServer, Project, BareJID, JobID) ->
    case odbc_organization:is_member2(LServer, Project, JobID, BareJID) of
        true ->
            Result = odbc_organization:get_task_jid(LServer, Project, JobID),
            JIDs = [R1 || {R1} <- Result],
            F = mochijson2:encoder([{utf8, true}]),
            {ok, iolist_to_binary(F({struct, [{<<"project">>, Project}, {<<"member">>, JIDs}]}))};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

get_task_ex(LServer, Project, BareJID, SelfJobID, JID, TargetJobID, Page) ->
    case odbc_organization:is_member2(LServer, Project, SelfJobID, BareJID) of
        true ->
            StartLine = integer_to_binary((binary_to_integer(Page) - 1) * binary_to_integer(?TASK_PAGE_ITEM_COUNT)),
            F = mochijson2:encoder([{utf8, true}]),
            Tasks = case odbc_organization:get_task(LServer, Project, JID, StartLine, ?TASK_PAGE_ITEM_COUNT) of
                        {ok, Result} ->
                            [{struct, [{<<"title">>, R1}, {<<"created_at">>, R2}, {<<"owner">>, R3}, {<<"joined_at">>, R4}]}
                                || {R1, R2, R3, R4} <- Result];
                        _ ->
                            []
                    end,
            {ok, iolist_to_binary(F({struct, [{<<"project">>, Project}, {<<"job_id">>, TargetJobID},
                {<<"jid">>, JID}, {<<"task">>, Tasks}]}) )};
        false ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.


%% ------------------------------------------------------------------
%% helper function.
%% ------------------------------------------------------------------

%% is_predefine_template(LServer, ProID) ->
%%     case odbc_organization:list_project(LServer, <<"0">>, true) of
%%         {ok, Result} ->
%%             case lists:keyfind(ProID, 1, Result) of
%%                 false ->
%%                     false;
%%                 _ ->
%%                     true
%%             end;
%%         {error, _Reason} ->
%%             failed
%%     end.

is_predefine_template(LServer, TemplateID) ->
    odbc_organization:is_template(LServer, TemplateID).

get_admin(LServer, ProID)->
    case odbc_organization:get_admin(LServer, ProID) of
        {ok, []} ->
            {false, empty};
        {ok, [{Admin}]} ->
            {true, Admin};
        {error, _Reason} ->
            {false, failed}
    end.

is_admin(LServer, BaseJID, ProID) ->
    case odbc_organization:get_admin(LServer, ProID) of
        {ok, [{Admin}]} ->
            if Admin =:= BaseJID -> true;
               true -> false
            end;
        _ ->
            false
    end.

push_message(ProID, Server, ToList, Type, Contents) ->
    From = jlib:jid_to_binary({ProID, Server, <<>>}),
    LangAttr = {<<"xml:lang">>, <<"en">>},
    FromAttr = {<<"from">>, From},
    TypeAttr = {<<"type">>, <<"chat">>},
    FromJID = jlib:make_jid(ProID, Server, <<>>),
    SubAttrs = [{<<"xmlns">>, ?NS_AFT_PROJECT}, {<<"type">>, Type}, {<<"projectid">>, ProID}],
    Packet = {xmlel, <<"message">>, [], [{xmlel, <<"sys">>, SubAttrs, [{xmlcdata, Contents}]}]},
    lists:foreach(
      fun(ToIn) ->
              {To} = ToIn,
              ToAttr = {<<"to">>, To},
              ejabberd_router:route(FromJID, jlib:binary_to_jid(To),
                                    Packet#xmlel{attrs = [FromAttr, ToAttr, TypeAttr, LangAttr]})
      end,
      ToList).

build_item([], [], Result) ->
    Result;
build_item([F1 | T1], [F2 | T2], Result) ->
    NF2 = if F2 =:= null -> <<>>; true -> F2 end,
    NewResult = if Result =:= <<>> ->
                        iolist_to_binary([Result, "\"", F1, "\":\"", NF2, "\""]);
                   true -> iolist_to_binary([Result, ", \"", F1, "\":\"", NF2, "\""])
                end,
    build_item(T1, T2, NewResult).

build_json([{Key, {ItemNameList, TupleItemValueList, AddBracket}} | Tail], Result) ->
    Count = length(TupleItemValueList),
    Array = lists:foldl(
              fun(E, AccIn) ->
                      AccIn1 = if AccIn =:= <<>> -> AccIn;
                                  true -> <<AccIn/binary, ",">>
                               end,
                      Item = build_item(ItemNameList, tuple_to_list(E), <<>>),
                      if Item =:= <<>> -> AccIn1;
                         true -> <<AccIn1/binary, "{", Item/binary, "}">>
                      end
              end,
              <<>>, TupleItemValueList),
    Temp = if (Result =:= <<>>) or (Array =:=<<>>) -> Result;
              true -> <<Result/binary, ",">>
           end,
    NewResult =
        if (Count > 1) or (AddBracket =:= true) ->
                iolist_to_binary( [Temp, "\"", Key, "\":[", Array, "]"  ]);
           true ->
                iolist_to_binary( [Temp, "\"", Key, "\":", Array ] )
        end,
    build_json(Tail, NewResult);
build_json([{Key, Value} | Tail], Result) ->
    build_json( [{Key, Value, false} | Tail], Result);
build_json([{Key, Value, AddBracket} | Tail], Result) ->
    Temp = if (Result =:= <<>>) or (Value =:=<<>>) -> Result;
              true -> <<Result/binary, ",">> end,
    NewResult =
        if (AddBracket =:= true) ->
                iolist_to_binary( [Temp, "\"", Key, "\":[\"", Value, "\"]"  ]);
           true ->
                iolist_to_binary( [Temp, "\"", Key, "\":\"", Value, "\"" ] )
        end,
    build_json(Tail, NewResult);
build_json([], Result) ->
    <<"{", Result/binary, "}">>.


parse_item([], _, Result) ->
    list_to_tuple(lists:reverse(Result));
parse_item([H | T], List, Result) ->
    R = case lists:keyfind(H, 1, List) of
            false -> Result;
            {H, Value} -> [Value | Result]
        end,
    parse_item(T, List, R).
parse_json_list(_, [], Result) ->
    lists:reverse(Result);
parse_json_list(Keys, [{struct, H} | T], Result) ->
    R = parse_item(Keys, H, []),
    parse_json_list(Keys, T, [R | Result]).


s3_bucket_url(PublicBucket) ->
    {ok, Host} = application:get_env(ejabberd, s3_host),
    Bucket = case PublicBucket of
                     true ->
                         {ok, Result} = application:get_env(ejabberd, s3_public_bucket),
                         Result;
                     _ ->
                         {ok, Result} = application:get_env(ejabberd, s3_bucket),
                         Result
                 end,

    %% which method is more effective ???
    %% list:flatten( ["http://" | [Bucket | ["." | Host]] ] ).
    "http://" ++ Bucket ++ "." ++ Host.

make_head_url( File ) ->
    URL = s3_bucket_url(true),
    case string:right(URL, 1) of
        "/" ->
            string:concat(URL, File);
        _ ->
            URL ++ "/" ++ File
    end.

photo_url(Photo) ->
    case Photo of
        null -> <<>>;
        <<>> -> <<>>;
        _ -> list_to_binary(make_head_url(binary_to_list(Photo)))
    end.