-module(odbc_organization).

%% API
-export([
    add_node/3,
    update_node/3,
    delete_node/2,
    node_exist/2,
    get_node/2,
    get_child_nodes/3,

    add_project/4,
    finish_project/2,
    add_employees/4,
    delete_employee/4,
    get_project/2,
    get_template/2,
    search_project/2,
    list_project/3,
    get_project_nodes/3,
    get_all/3,
    set_photo/3,
    change_admin/3,
    is_project_name_exist/2,
    is_member/3,
    is_member/4,

    list_link_project/2,
    is_link_member/4,
    subscribe/3,
    subscribed/3,
    unsubscribed/3,
    unsubscribe/3,
    is_subscribe_exist/3,

    get_task_jid/3,
    is_child/6,
    is_child/4,
    get_task/5,
    delete_task/3]).

-include("jlib.hrl").
-include("organization.hrl").

-type pro_type() :: binary() | integer().

%% Columns: ["jid", "id", "name", ...]
-spec get_all(binary(), integer(), list()) -> {ok, list()} | {error, _}.
get_all(LServer, Project, Columns) ->
    HasJID = lists:member("jid", Columns),

    Query = if
                HasJID =:= true ->
                    QueryColumns = string:sub_string(
                        lists:flatten(lists:map(fun(E) -> if E =:= "jid" -> ",ou.jid"; true -> [",o."|E ] end end, Columns)), 2),
                    ["select ", QueryColumns, " from organization_user as ou, organization as o where o.id=ou.organization ",
                        " and o.project='", Project, "';"];
                HasJID =:= false ->
                    QueryColumns = string:sub_string(lists:flatten(lists:map(fun(E) -> [","|E]  end, Columns)), 2),
                    ["select ", QueryColumns, " from organization where project='", Project, "';"]
            end,

    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Rs} ->
            {ok, Rs};
        Reason ->
            {error, Reason}
    end.

-spec is_link_member(binary(), odbc_organization:pro_type(), binary(), odbc_organization:pro_type()) -> true | false.
is_link_member(LServer, ProjectSelf, JID, ProjectTarget) ->
    Query = ["select count(id1) from project_link as pl join organization as o on ((o.project=pl.id1 and pl.id2='", ProjectTarget,
        "') or (o.project=pl.id2 and pl.id1='", ProjectTarget, "')) and o.project='", ProjectSelf, "' join organization_user as ou ",
        "on o.id = ou.organization and ou.jid='", escape(JID), "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected,[<<"count(id1)">>],[{<<"2">>}]} ->
            true;
        _ ->
            false
    end.

-spec get_project_nodes(binary(), odbc_organization:pro_type(), list()) -> {ok, list()} | {error, _}.
get_project_nodes(LServer, Project, Columns) ->
    QueryColumns = string:sub_string(lists:flatten(lists:map(fun(E) -> [","|E]  end, Columns)), 2),
    Query = ["select ", QueryColumns, " from organization where project='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Rs } ->
            {ok, Rs};
        Reason ->
            {error, Reason}
    end.

-spec is_member(binary(), odbc_organization:pro_type(), binary()) -> true | false.
is_member(LServer, Project, JID) ->
    Query = ["select count(ou.id) from organization_user as ou join organization as o on ou.organization = o.id and o.project='",
        Project, "' where ou.jid='", escape(JID), "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{<<"0">>}]} ->
            false;
        {selected, _, [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

-spec is_member(binary(), binary(), binary(), binary()) -> true | false.
is_member(LServer, Project, JID, JobID) ->
    Query = ["select count(ou.id) from organization_user as ou join organization as o on ou.organization =o.id ",
        " and ou.jid='", escape(JID), "' and o.id='", JobID, "' and o.project='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

%% %% get a random member in department, then get it all parent job id.
%% -spec get_department_member_parent(binary(), odbc_organization:pro_type(), binary(), binary()) -> {ok, list()} | {error, _}.
%% get_department_member_parent(LServer, Project, DepartmentID, DepartmentLevel) ->
%%     Query = ["select o1.id from organization as o1 join (select lft, rgt, project from organization where project='", Project,
%%         "' and department_id='", escape(DepartmentID), "' and department_level='", escape(DepartmentLevel),
%%         "' limit 1) as o2 on o1.lft < o2.lft and o1.rgt > o2.rgt where o1.project='", Project, "';" ],
%%     case ejabberd_odbc:sql_query(LServer, Query) of
%%         {selected, [<<"id">>], Rs} ->
%%             {ok, Rs};
%%         Reason ->
%%             {error, Reason}
%%     end.

-spec get_node(binary(), binary()) -> {ok, #node{}} | {error, _}.
get_node(LServer, Job) ->
    Query = ["select id, name, lft, rgt, depth, department, department_level, department_id, project from organization where id ='", Job, "';" ],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{ID, Name, Left, Right, Depth, Department, DepartmentLevel, DepartmentID, Project}]} ->
            {ok, #node{id=ID, name=Name, lft=Left, rgt = Right, depth = Depth, department = Department,
                department_level = DepartmentLevel, department_id = DepartmentID, project = Project}};
        Reason ->
            {error, Reason}
    end.

-spec get_child_nodes(binary(), #node{}, list()) -> {ok, _} | {error, _}.
get_child_nodes(LServer, #node{id = ID, lft = undefined, project = Project}, Columns) ->
    QueryColumns = string:sub_string(lists:flatten(lists:map(fun(E) -> [",o1."|E]  end, Columns)), 2),
    Query = ["select ", QueryColumns, " from organization as o1 join organization as o2 ",
        " on o1.lft>o2.lft and o1.rgt<o2.rgt and o2.id='", ID, "' where o1.project='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, []} ->
            {error, no_exists};
        {selected, _, Rs} ->
            {ok, Rs};
        Reason ->
            {error, Reason}
    end.

-spec delete_node(binary(), binary()) -> ok | {error, _}.
delete_node(LServer, NodeId) ->
    F = fun() ->
        Q1 = ["select lft,rgt,project from organization where id=", NodeId, ";"],
        {selected, [<<"lft">>, <<"rgt">>, <<"project">>], [{Left, Right, Project}]} = ejabberd_odbc:sql_query_t(Q1),
        case binary_to_integer(Right) - binary_to_integer(Left) of
            1 ->
                Q2 = [["delete from organization where id = ", NodeId, ";"],
                    ["update organization set lft=lft-2 where lft > ", Left, " and project = ", Project, ";"],
                    ["update organization set rgt=rgt-2 where rgt > ", Right, " and project = ", Project, ";"]],
                sql_list(Q2);
            _ ->
                error
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok} ->
            ok;
        {atomic, error} ->
            {error, not_valid};
        Reason ->
            {error, Reason}
    end.

-spec update_node(binary(), #node{}, binary()) -> ok | {error, _}.
update_node(LServer, #node{id = Id}, NewName) ->
    Query = ["update organization set name = ", escape(NewName), " where id = ", Id, ";"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {updated, 1} ->
            ok;
        Reason ->
            {error, Reason}
    end.

-spec add_node(binary(), integer(), #node{}) -> {ok, #node{}} | {error, _}.
add_node(LServer, ParentId, #node{name = Name, department = Department, department_level = DepartmentLevel, department_id = DepartmentID} = _Node) ->
    TimeString = now_random(),
    F = fun() ->
        {selected, [<<"lft">>, <<"rgt">>, <<"depth">>, <<"project">>], [{Left, Right, Depth, Project}]} =
            ejabberd_odbc:sql_query_t(["select lft,rgt,depth,project from organization where id=", ParentId, ";"]),
        {Left1, Right1, Depth1} = {plus(Left, 1), plus(Left, 2), plus(Depth, 1)},
        Query = [
            ["update organization set lft=lft+2 where lft > ", Left, " and project = ", Project, ";"],
            ["update organization set rgt=rgt+2 where rgt > ", Left, " and project = ", Project, ";"],
            ["insert into organization(name,lft,rgt,depth,department,department_level,department_id,project) values('",
                escape(Name), "',", Left1, ",", Right1, ",", Depth1, ",'", escape(Department),
                "','", DepartmentLevel, "','", DepartmentID, "',", Project, ");"],
            ["update project set job_tag='", TimeString, "' where id='", Project, "';"]

        ],
        sql_list(Query),
        {selected, [<<"id">>], [{Id}]} = ejabberd_odbc:sql_query_t(["select last_insert_id() as id;"]),
        {ok, #node{id = Id, lft = Left1, rgt = Right1, depth = Depth1, name = Name, department = Department}}
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {ok, Rs}} ->
            {ok, TimeString, Rs};
        Reason ->
            {error, Reason}
    end.

-spec node_exist(binary(), #node{}) -> {ok, _} | {error, _}.
node_exist(LServer, #node{id = ID, project = Project}) ->
    Query = ["select id from organization where id ='", ID, "' and project='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"id">>], []} ->
            {ok, false};
        {selected, [<<"id">>], _Rs} ->
            {ok, true};
        Reason ->
            {error, Reason}
    end.

-spec add_employees(binary(), binary(), binary(), list()) -> {ok, _} | {error, _}.
add_employees(LServer, Project, _BareJID, List) ->
    TimeString = now_random(),
    Init = <<"insert into organization_user(organization, jid) values">>,
    Temp = lists:foldl(fun(E, AccIn) ->
        {Id, Jid} = E,
        AccIn1 = if
                     AccIn =:= Init -> Init;
                     true -> <<AccIn/binary, ",">>
                 end,
        <<AccIn1/binary, "(", Id/binary, ", '", Jid/binary, "')">>
    end,
    Init,
    List),
    Query1 = <<Temp/binary, ";">>,
    Size = lists:flatlength(List),
    Query2 = ["update project set member_tag='", TimeString, "' where id='", Project, "';"],

    F = fun() ->
        case ejabberd_odbc:sql_query_t(Query1) of
            {updated, Size} ->
                ejabberd_odbc:sql_query_t(Query2),
                ok;
            Reason ->
                Reason
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok} ->
            {ok, TimeString};
        Reason ->
            {error, Reason}
    end.

-spec delete_employee(binary(), odbc_organization:pro_type(), binary(), {binary(), binary()}) -> {ok, _} | {error, _}.
delete_employee(LServer, Project, _BareJID, {JID, Job}) ->
    TimeString = now_random(),
    Query1 = ["delete from organization_user where organization=", Job, " and jid='", JID, "';"],
    Query2 = ["update project set member_tag='", TimeString, "' where id='", Project, "';"],

    F = fun() ->
        {updated, 1} = ejabberd_odbc:sql_query_t(Query1),
        {updated, 1} = ejabberd_odbc:sql_query_t(Query2),
        ok
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok} ->
            {ok, TimeString};
        Reason ->
            {error, Reason}
    end.

-spec add_project(binary(), #project{}, binary(), binary) -> {ok, #project{}, #node{}} | {error, _}.
add_project(LServer, #project{name = Name, description = Desc, admin = Admin}, TemplateId, Job) ->
    TimeString = now_random(),
    Negative_TemplateId = integer_to_binary( 0 - binary_to_integer(TemplateId) ),
        F = fun() ->
        {selected, [<<"photo">>], [{Photo}]} = ejabberd_odbc:sql_query_t(["select photo from template where id='", TemplateId, "';"]),
        Query1 = ["insert into project(name,description,photo,admin,job_tag,member_tag, link_tag) values('",
            escape(Name), "','", escape(Desc), "','", Photo, "','",
            escape(Admin), "','", TimeString, "','", TimeString, "','", TimeString, "');"],
        ejabberd_odbc:sql_query_t(Query1),
        {selected, [<<"id">>, <<"start_at">>], [{Id, StartTime}]} = ejabberd_odbc:sql_query_t(["select id, start_at from project where id=last_insert_id();"]),
        Query2 = ["insert into organization(name,lft,rgt,depth,department,department_level,department_id, project) select name,lft,rgt,depth,department,",
            "department_level, department_id,", Id, " from organization where project =", Negative_TemplateId, ";"],
        ejabberd_odbc:sql_query_t(Query2),
        Query3 = ["select o1.id, o1.name, o1.lft, o1.rgt, o1.department, o1.department_level, o1.department_id from organization o1, organization o2 where o1.lft=o2.lft and o2.id='",
            Job, "' and o1.project='", Id, "';"],
        {selected, _, [{NewJob, JobName, Left, Right, Department, DepartmentLevel, DepartmentID}]} = ejabberd_odbc:sql_query_t(Query3),
        Query4 = ["insert into organization_user(organization, jid) values('", NewJob, "', '", Admin, "');"],
        ejabberd_odbc:sql_query_t(Query4),
        {Id, StartTime, Photo, NewJob, JobName, Left, Right, Department, DepartmentLevel, DepartmentID}
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {Id, StartTime, Photo, NewJob, JobName, Left, Right, Department, DepartmentLevel, DepartmentID}} ->
            {ok, #project{id = Id, name = Name, description = Desc, photo = Photo, job_tag = TimeString, start_at = StartTime},
                #node{id = NewJob, name = JobName, lft = Left, rgt = Right, department = Department, department_level = DepartmentLevel, department_id = DepartmentID}};
        Reason ->
            {error, Reason}
    end.

-spec finish_project(binary(), odbc_organization:pro_type()) -> ok | {error, _}.
finish_project(LServer, Project) ->
    F = fun() ->
        Query1 = ["select count(groupid) from groupinfo where project='", Project, "' and type='2' and status='1';"],
        case ejabberd_odbc:sql_query_t(Query1) of
            {selected, _, [{<<"0">>}]} ->
                {selected, [<<"time">>], [{Time}]} = ejabberd_odbc:sql_query_t(["select current_timestamp() as time;"]),
                Query2 = ["update project set status='0', end_at='", Time, "' where id=", Project],
                R = ejabberd_odbc:sql_query_t(Query2),
                {R, Time};
            {selected, _, _C} ->
                continue;
            Error ->
                Error
        end
        end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {{updated, _Counter}, Time}} ->
            {ok, Time};
        {atomic, continue} ->
            {continue, task};
        Reason ->
            {error, Reason}
    end.

-spec list_project(binary(), binary(), boolean()) -> {ok, list()} | {error, _}.
list_project(LServer, JID, IsTemplate) ->
    case IsTemplate of
        true ->
            case ejabberd_odbc:sql_query(LServer, ["select id, name, description, photo, job_tag from template;"]) of
                {selected, [<<"id">>, <<"name">>, <<"description">>, <<"photo">>,<<"job_tag">>], Rs} ->
                    {ok, Rs};
                Reason ->
                    {error, Reason}
            end;
        _ ->
            Query = ["select p.id, p.name, p.description, p.photo, p.status, p.admin, p.start_at, p.end_at, p.job_tag, p.member_tag, p.link_tag ",
                "from project as p, organization_user as ou, organization as o where o.id=ou.organization and ou.jid='", escape(JID),
                "' and p.id=o.project;"],
            case ejabberd_odbc:sql_query(LServer, Query) of
                {selected, [<<"id">>, <<"name">>, <<"description">>, <<"photo">>, <<"status">>, <<"admin">>,
                    <<"start_at">>, <<"end_at">>, <<"job_tag">>, <<"member_tag">>, <<"link_tag">>], Rs} ->
                    {ok, Rs};
                Reason ->
                    {error, Reason}
            end
    end.

-spec list_link_project(binary(), odbc_organization:pro_type()) -> {ok, list()} | {error, _}.
list_link_project(LServer, Project) ->
    Query = ["select pl1.id2, p.name, p.photo, p.job_tag, p.member_tag from project_link as pl1 join project_link as pl2 ",
        "on pl2.id2='", Project, "' and pl2.id1=pl1.id2 join project as p on pl1.id2 = p.id where pl1.id1='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"id2">>, <<"name">>, <<"photo">>, <<"job_tag">>, <<"member_tag">>], Rs } ->
            {ok, Rs};
        Reason ->
            {error, Reason}
    end.


-spec set_photo(binary(), odbc_organization:pro_type(), binary()) -> ok | {error, _}.
set_photo(LServer, ProID, Photo) ->
    Query = ["update project set photo='", escape(Photo), "' where id='", ProID, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {updated, 1} ->
            ok;
        Reason ->
            {error, Reason}
    end.

-spec get_project(binary(), binary()) -> {ok, #project{}} | {error, _}.
get_project(LServer, Project) ->
    Query = ["select id, name, description, photo, status, admin, start_at, end_at, job_tag, member_tag, link_tag, work_url ",
        " from project where id='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{ID, Name, Description, Photo, Status, Admin, StartAt, EndAt, JobTag, MemberTag, LinkTag, WorkUrl}]} ->
            {ok, #project{id=ID, name=Name, description = Description, photo = Photo, status = Status, admin = Admin,
            start_at = StartAt, end_at = EndAt, job_tag = JobTag, member_tag = MemberTag, link_tag = LinkTag, work_url = WorkUrl}};
        {selected, _, []} ->
            {ok, #project{}};
        Reason ->
            {error, Reason}
    end.

-spec get_template(binary(), binary()) -> {ok, #project{}} | {error, _}.
get_template(LServer, Template) ->
    Query = ["select id, name, description, photo, job_tag from template where id='", Template, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{ID, Name, Description, Photo, JobTag}]} ->
            {ok, #project{id=ID, name=Name, description = Description, photo = Photo, job_tag = JobTag}};
        {selected, _, []} ->
            {ok, #project{}};
        Reason ->
            {error, Reason}
    end.

-spec is_project_name_exist(binary(), binary()) -> true | false.
is_project_name_exist(LServer, ProjectName) ->
    Query = ["select count(id) from project where name='", escape(ProjectName), "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"count(id)">>], [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

-spec subscribe(binary(), odbc_organization:pro_type(), odbc_organization:pro_type()) -> ok | {error, _}.
subscribe(LServer, ProSource, ProTarget) ->
    Query = ["insert into project_link(id1, id2) values('", ProSource, "', '", ProTarget, "');"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {updated, 1} ->
            ok;
        Reason ->
            {error, Reason}
    end.

-spec subscribed(binary(), odbc_organization:pro_type(), odbc_organization:pro_type()) -> ok | {error, _}.
subscribed(LServer, ProSource, ProTarget) ->
    TimeString = now_random(),
    Query1 = ["insert into project_link(id1, id2) values('", ProSource, "', '", ProTarget, "');"],
    Query2 = ["update project set link_tag='", TimeString, "' where id in('", ProSource, "','", ProTarget, "');"],

    F = fun() ->
                case ejabberd_odbc:sql_query_t(Query1) of
                    {updated, 1} ->
                        ejabberd_odbc:sql_query_t(Query2),
                        {ok, TimeString};
                    Reason ->
                        Reason
                end
        end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic,  {ok, TimeString}} ->
            {ok, TimeString};
        Reason ->
            {error, Reason}
    end.

-spec unsubscribed(binary(), odbc_organization:pro_type(), odbc_organization:pro_type()) -> ok | {error, _}.
unsubscribed(LServer, ProSource, ProTarget) ->
    Query =["delete from project_link where id1='", ProSource, "' and id2='", ProTarget, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {updated, 1} ->
            ok;
        {updated, 0} ->
            ok;
        Reason ->
            {error, Reason}
    end.

%% @doc make sure source and target project is subscribed for each other.
-spec unsubscribe(binary(), odbc_organization:pro_type(), odbc_organization:pro_type()) -> ok | {error, _}.
unsubscribe(LServer, ProSource, ProTarget) ->
    TimeString = now_random(),
    Query1 = ["delete from project_link where id1='", ProSource, "' and id2='", ProTarget, "';"],
    Query2 = ["delete from project_link where id1='", ProTarget, "' and id2='", ProSource, "';"],
    Query3 = ["update project set link_tag='", TimeString, "' where id in('", ProSource, "','", ProTarget, "');"],

    F = fun() ->
                ejabberd_odbc:sql_query_t(Query1),
                ejabberd_odbc:sql_query_t(Query2),
                ejabberd_odbc:sql_query_t(Query3),
                ok
        end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok} ->
            {ok, TimeString};
        Reason ->
            {error, Reason}
    end.

-spec is_subscribe_exist(binary(), odbc_organization:pro_type(), odbc_organization:pro_type()) -> true | false.
is_subscribe_exist(LServer, ProSource, ProTarget) ->
    Query = ["select count(id1) from project_link where id1='", ProSource, "' and id2='", ProTarget, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"count(id1)">>], [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

-spec search_project(binary(), binary()) -> {ok, list} | {error, _}.
search_project(LServer, ProjectName) ->
    Query = ["select id, name from project where name like '%", ProjectName, "%';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Rs} ->
            {ok, Rs};
        Reason ->
            {error, Reason}
    end.

change_admin(LServer, Project, Admin) ->
    case ejabberd_odbc:sql_query(LServer, ["update project set admin='", escape(Admin),
        "' where id='", Project, "';"]) of
        {updated, 1} ->
            ok;
        Reason ->
            {error, Reason}
    end.

%% get project jids who has task.
get_task_jid(LServer, Project, _JobID) ->
    Query = ["select gu.jid from groupuser as gu join groupinfo as gi on gu.groupid=gi.groupid and gi.project='", Project,
        "' and gi.type=2 and gi.status=1 group by gu.jid;"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, R} ->
            R;
        Reason ->
            {error, Reason}
    end.

%% check (JID2, Jobs) is child job of (JID1, Job1) in Project.
is_child(LServer, Project, JID1, Job1, JID2, Job2) ->
    Query = ["select count(ou1.id) from organization_user as ou1 join organization as o1 on ou1.organization=o1.id join ",
        "organization as o on o1.lft>o.lft and o1.rgt<o.rgt and o1.project='", Project, "' join organization_user as ou ",
        "on o.id=ou.organization and ou.organization='", Job1, "' and ou.jid='", JID1, "' where ou1.jid='",
        JID2,"' and ou1.organization='", Job2, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

%% check Job2 is child job of Job1 in Project
is_child(LServer, Project, Job1, Job2) ->
    Query = ["select count(o1.id) from organization as o1 join organization as o2 on o1.lft < o2.lft and o1.rgt > o2.rgt and o2.id='",
        Job2, "' and o1.project=o2.project and o2.project='", Project,"' where o1.id='", Job1, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

get_task(LServer, Project, JID, StartLine, Count) ->
    Query =["select gi.groupid, gi.name, gi.owner, gi.created_at, gu.joined_at from groupuser as gu join groupinfo as gi on ",
        "gi.groupid=gu.groupid and gi.type=2 and gi.project='", Project, "' and gi.status=1 where gu.jid='",
        JID, "' order by gu.joined_at desc limit ", StartLine, ",", Count, ";"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, Result} ->
            {ok, Result};
        Reason ->
            {error, Reason}
    end.

delete_task(LServer, Project, JID) ->
    case gen_mod:is_loaded(LServer, mod_groupchat) of
        true ->
            SJID = escape(JID),
            Query = ["select t.groupid from (select gu1.groupid, count(gu1.groupid) as count from groupuser as gu1 join groupuser as gu2 ",
                "on gu1.groupid=gu2.groupid join groupinfo as gi on gu2.groupid=gi.groupid and gi.project='", Project,"' and gi.status='1' ",
                "and gi.type='2' and gi.owner != '", SJID, "' where gu2.jid='", SJID, "' group by gu1.groupid) as t where t.count = 2;"],
            F = fun() ->
                {selected, _, JoinPairTasks} = ejabberd_odbc:sql_query_t(Query),
                Length = length(JoinPairTasks),
                if
                    Length > 0 -> {continue, JoinPairTasks};
                    true ->
                        Q1 = ["update groupinfo set status='2' where project='", Project, "' and status='1' and owner='", escape(JID), "';"],
                        Q2 = ["delete gu.* from groupuser as gu join groupinfo as gi on gu.groupid = gi.groupid and gi.project='",
                            Project, "' where gu.jid='", escape(JID), "';"],
                        ejabberd_odbc:sql_query_t(Q1),
                        ejabberd_odbc:sql_query_t(Q2),
                        ok
                end
            end,
            case ejabberd_odbc:sql_transaction(LServer, F) of
                {atomic, ok} ->
                    ok;
                {atomic, {continue, Result}} ->
                    PairTasks = [R || { R } <- Result],
                    {continue, PairTasks};
                _ ->
                    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        _ ->
            ok
    end.


%% =================================================================================
%% helper function
%% =================================================================================

-spec sql_list([string()]) -> ok.
sql_list(QueryList) ->
    lists:foreach(fun(X) ->
                          ejabberd_odbc:sql_query_t(X)
                  end, QueryList).

-spec plus(binary(), integer()) -> binary().
plus(I, P) ->
    integer_to_binary(binary_to_integer(I) + P).

now_random() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
    Ran = random:uniform(9999),
    list_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ@~4..0w",
                                 [Year, Month, Day, Hour, Minute, Second, Ran])).

escape(Name) ->
    ejabberd_odbc:escape(Name).