%% ====================================================================================
%% groupchat module
%% doc here: https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#
%%
%% ====================================================================================

-module(mod_groupchat).

-behaviour(gen_mod).

-export([start/2, stop/1, process_iq/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("organization.hrl").

-define(NS_GROUPCHAT, <<"aft:groupchat">>).

start(Host, _Opts) ->
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host,
        ?NS_GROUPCHAT, ?MODULE, process_iq, no_queue).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_GROUPCHAT).


%% ================================================
%% iq handler
%% ================================================

process_iq(From, To, #iq{xmlns = ?NS_GROUPCHAT, type = _Type, sub_el = SubEl} = IQ) ->
    case SubEl of
        #xmlel{name = <<"query">>} ->
            case xml:get_tag_attr_s(<<"query_type">>, SubEl) of
                <<"create_group">> ->
                    create_group(From, To, IQ);
                <<"add_member">> ->
                    add_members(From, To, IQ);
                <<"get_groups">> ->
                    get_groups(From, To, IQ);
                <<"get_members">> ->
                    get_members(From, To, IQ);
                <<"get_groupinfo">> ->
                    get_groupinfo(From, To, IQ);
                <<"group_member">> ->
                    create_and_add(From, To, IQ);
                <<"set_groupname">> ->
                    set_groupname(From, To, IQ);
                <<"set_avatar">> ->
                    set_avatar(From, To, IQ);
                <<"remove_members">> ->
                    remove_members(From, To, IQ);
                <<"dismiss_group">> ->
                    dismiss_group(From, To, IQ);
                <<"set_nickname">> ->
                    set_nickname(From, To, IQ);
                <<"complete_task">> ->
                    complete_task(From, To, IQ);
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
            end;
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;

process_iq(_, _, IQ) ->
    #iq{sub_el = SubEl} = IQ,
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.


%% ================================================
%% api
%% ================================================


%% @doc get_members by groupid
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#2-get-members-of-a-group-by-groupdid
get_members(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    case odbc_groupchat:is_user_in_group(LServer, UserJid, GroupId) of
        true ->
            case odbc_groupchat:get_members_by_groupid(LServer, GroupId) of
                {ok, MembersInfoList} ->
                    IQ#iq{type = result, sub_el = [SubEl#xmlel{children =
                    [{xmlcdata, members_to_json(MembersInfoList)}]}]};
                {error, _} ->
                    IQ#iq{type = error, sub_el = []}
            end;
        _ ->
            make_error_reply(IQ, <<"15101">>)
    end.

%% @doc get groups by jid
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#1-get-all-groups
get_groups(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    case odbc_groupchat:get_groups_by_jid(LServer, UserJid) of
        {ok, Rs} when is_list(Rs) ->
            IQ#iq{type = result, sub_el = [SubEl#xmlel{children = [{xmlcdata, grouplist_to_json(Rs)}]}]};
        {error, _} ->
            make_error_reply(IQ, <<"15102">>)
    end.

%% @doc get groupinfo by groupid
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#10-get-group-info-by-groupid
get_groupinfo(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    case odbc_groupchat:is_user_in_group(LServer, UserJid, GroupId) of
        true ->
            case odbc_groupchat:get_groupinfo_by_groupid(LServer, GroupId) of
                {ok, Group} ->
                    IQ#iq{type = result, sub_el = [SubEl#xmlel{children =
                    [{xmlcdata, group_to_json(Group)}]}]};
                {error, _} ->
                    make_error_reply(IQ, <<"15102">>)
            end;
        _ ->
            make_error_reply(IQ, <<"15101">>)
    end.


%% @doc create group
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#3-create-a-group
create_group(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    GroupName = xml:get_tag_attr_s(<<"groupname">>, SubEl),
    GroupType = get_group_type(SubEl),
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    Avatar = xml:get_tag_attr_s(<<"avatar">>, SubEl),
    Group = #group{master = UserJid, groupname = GroupName, type = GroupType, project = Project, avatar = Avatar},
    case {GroupType, Project} of
        {?TASK_GROUP, <<>>} ->
            IQ#iq{type = error};
        {?TASK_GROUP, _} ->
            case odbc_groupchat:is_in_project(LServer, [UserJid], Project) of
                true ->
                    do_create_group(LServer, IQ, Group);
                _ ->
                    make_error_reply(IQ, <<"15103">>)
            end;
        _ ->
            do_create_group(LServer, IQ, Group)
    end.


%% @doc add members
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#4-add-memebers-to-a-group
add_members(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    case odbc_groupchat:is_user_in_group(LServer, UserJid, GroupId) of
        true ->
            MembersList = mochijson2:decode(xml:get_tag_cdata(SubEl)),
            case MembersList of
                [] -> make_error_reply(IQ, <<"15105">>);
                _ ->
                    case odbc_groupchat:get_members_by_groupid(LServer, GroupId) of
                        {ok, MembersInfoList} when is_list(MembersInfoList) ->
                            case MembersInfoList of
                                [] ->
                                    make_error_reply(IQ, <<"15105">>);
                                _ ->
                                    ExistsMembers = [Jid || {Jid, _} <- MembersInfoList],
                                    NewMembers = lists:filter(fun(X) ->
                                        not lists:member(X, ExistsMembers)
                                    end, MembersList),
                                    do_add_members(LServer, GroupId, ExistsMembers, NewMembers, IQ, SubEl)
                            end;
                        {error, _} ->
                            make_error_reply(IQ, <<"15102">>)
                    end
            end;
        _ ->
            make_error_reply(IQ, <<"15101">>)
    end.

%% @doc create and add members to group
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#5-create-a-group-and-add-memebers-to-it
create_and_add(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    GroupName = xml:get_tag_attr_s(<<"groupname">>, SubEl),
    GroupType = get_group_type(SubEl),
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    Avatar = xml:get_tag_attr_s(<<"avatar">>, SubEl),
    MembersList = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    Group = #group{master = UserJid, groupname = GroupName,
        type = GroupType, project = Project, avatar = Avatar},
    case {MembersList, GroupType, Project} of
        {[], _, _} ->
            make_error_reply(IQ, <<"15105">>);
        {_, ?TASK_GROUP, <<>>} ->
            make_error_reply(IQ, <<"15105">>);
        {_, ?TASK_GROUP, _} ->
            case odbc_groupchat:is_in_project(LServer, [UserJid | MembersList], Project) of
                true ->
                    do_create_add(LServer, IQ, Group, MembersList);
                _ ->
                    make_error_reply(IQ, <<"15103">>)
            end;
        _ ->
            do_create_add(LServer, IQ, Group, MembersList)
    end.

%% @doc set group name
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#6-modify-group-name
set_groupname(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    case odbc_groupchat:get_members_by_groupid(LServer, GroupId) of
        {ok, MembersInfoList} ->
            MembersList = [Jid || {Jid, _} <- MembersInfoList],
            case lists:member(UserJid, MembersList) of
                true ->
                    GroupName = xml:get_tag_attr_s(<<"groupname">>, SubEl),
                    case odbc_groupchat:set_groupname(LServer, GroupId, GroupName) of
                        ok ->
                            push_groupinfo(#group{groupid = GroupId, groupname = GroupName},
                                LServer, MembersList, <<"rename">>),
                            Res = SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_GROUPCHAT},
                                {<<"groupid">>, GroupId}, {<<"groupname">>, GroupName}]},
                            IQ#iq{type = result, sub_el = [Res]};
                        {error, _} ->
                            make_error_reply(IQ, <<"15102">>)
                    end;
                false ->
                    make_error_reply(IQ, <<"15101">>)
            end
    end.

%% @doc set group avatar
set_avatar(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    case odbc_groupchat:get_members_by_groupid(LServer, GroupId) of
        {ok, MembersInfoList} ->
            MembersList = [Jid || {Jid, _} <- MembersInfoList],
            case lists:member(UserJid, MembersList) of
                true ->
                    Avatar = xml:get_tag_attr_s(<<"avatar">>, SubEl),
                    case odbc_groupchat:set_avatar(LServer, GroupId, Avatar) of
                        ok ->
                            push_groupinfo(#group{groupid = GroupId, avatar = Avatar},
                                LServer, MembersList, <<"avatar">>),
                            IQ#iq{type = result};
                        {error, _} ->
                            make_error_reply(IQ, <<"15102">>)
                    end;
                false ->
                    make_error_reply(IQ, <<"15101">>)
            end
    end.

%% @doc set nickname in group
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#9-a-memeber-modify-his-group-nickname
set_nickname(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    NickName = xml:get_tag_attr_s(<<"nickname">>, SubEl),
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    case odbc_groupchat:set_nickname_in_group(LServer, GroupId, UserJid, NickName) of
        ok ->
            case odbc_groupchat:get_members_by_groupid(LServer, GroupId) of
                {ok, MembersInfoList} ->
                    push_groupmember(GroupId, <<>>, <<>>, LServer, [Jid || {Jid, _} <- MembersInfoList],
                        [{UserJid, NickName}], <<"rename">>);
                _ -> nopush
            end,
            IQ#iq{type = result, sub_el = [SubEl]};
        _ ->
            make_error_reply(IQ, <<"15102">>)
    end.

%% @doc complete task
complete_task(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    case odbc_groupchat:is_user_own_group(LServer, UserJid, GroupId) of
        true ->
            case odbc_groupchat:complete_task(LServer, GroupId) of
                ok ->
                    case odbc_groupchat:get_groupinfo_by_groupid(LServer, GroupId) of
                        {ok, Group} ->
                            {ok, R} = odbc_groupchat:get_members_by_groupid(LServer, GroupId),
                            push_groupinfo(Group, LServer,
                                [Jid || {Jid, _} <- R], <<"complete">>);
                        _ ->
                            error
                    end,
                    IQ#iq{type = result, sub_el = SubEl};
                _ ->
                    make_error_reply(IQ, <<"15102">>)
            end;
        _ ->
            make_error_reply(IQ, <<"15104">>)
    end.

%% @doc remove members from group
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#8-remove-members-from-a-group-a-user-quit-a-group-or-an-owner-remove-members
remove_members(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    MembersList = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    case odbc_groupchat:is_user_own_group(LServer, UserJid, GroupId) of
        true ->
            do_remove_members(IQ, LServer, GroupId, MembersList);
        _ ->
            case odbc_groupchat:is_user_in_group(LServer, UserJid, GroupId) of
                true ->
                    case MembersList of
                        [UserJid] ->
                            do_remove_members(IQ, LServer, GroupId, MembersList);
                        _ ->
                            make_error_reply(IQ, <<"15104">>)
                    end;
                _ ->
                    make_error_reply(IQ, <<"15101">>)
            end
    end.

%% @doc dismiss group, must be owner of the group
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#7-dismiss-a-group-by-its-owner
dismiss_group(#jid{luser = LUser, lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    GroupId = xml:get_tag_attr_s(<<"groupid">>, SubEl),
    UserJid = jlib:jid_to_binary({LUser, LServer, <<>>}),
    case odbc_groupchat:is_user_own_group(LServer, UserJid, GroupId) of
        true ->
            case odbc_groupchat:get_members_by_groupid(LServer, GroupId) of
                {ok, MembersInfoList} ->
                    case odbc_groupchat:dismiss_group(LServer, GroupId, MembersInfoList) of
                        ok ->
                            push_groupinfo(#group{groupid = GroupId, groupname = <<>>}, LServer,
                                [Jid || {Jid, _} <- MembersInfoList], <<"dismiss">>),
                            IQ#iq{type = result, sub_el = [SubEl]};
                        _ ->
                            make_error_reply(IQ, <<"15102">>)
                    end;
                _ ->
                    make_error_reply(IQ, <<"15102">>)
            end;
        _ ->
            make_error_reply(IQ, <<"15104">>)
    end.


%% ================================================
%% helper
%% ================================================

do_create_group(LServer, #iq{sub_el = SubEl} = IQ, Group) ->
    case odbc_groupchat:create_group(LServer, Group) of
        {ok, GroupId} ->
            Res = SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_GROUPCHAT}], children =
            [{xmlcdata, group_to_json(Group#group{groupid = GroupId, status = 1})}]},
            IQ#iq{type = result, sub_el = [Res]};
        {error, _} ->
            IQ#iq{type = error, sub_el = []}
    end.

do_create_add(LServer, #iq{sub_el = SubEl} = IQ, #group{master = UserJid,
    groupname = GroupName, type = GroupType} = Group, MembersList) ->
    case odbc_groupchat:create_and_add(LServer, Group, MembersList) of
        {ok, GroupMembersInfo} ->
            [{GroupId, _, _} | _] = GroupMembersInfo,
            MembersInfoList = [{Jid, NickName} || {_, Jid, NickName} <- GroupMembersInfo],
            push_groupmember(GroupId, GroupName, UserJid, LServer, [Jid || {Jid, _} <- MembersInfoList],
                MembersInfoList, <<"add">>),
            IQ#iq{type = result, sub_el = [SubEl#xmlel{
                attrs = [{<<"xmlns">>, ?NS_GROUPCHAT},
                    {<<"groupid">>, GroupId},
                    {<<"query_type">>, <<"group_member">>},
                    {<<"groupname">>, GroupName},
                    {<<"master">>, UserJid},
                    {<<"type">>, GroupType}
                ],
                children = [{xmlcdata, members_to_json(MembersInfoList)}]}]};
        {error, _} ->
            make_error_reply(IQ, <<"15102">>)
    end.

do_add_members(LServer, GroupId, ExistsMembers, NewMembers, IQ, SubEl) ->
    case NewMembers of
        [] ->
            IQ#iq{type = result, sub_el =
            [SubEl#xmlel{
                attrs = [{<<"xmlns">>, ?NS_GROUPCHAT}, {<<"groupid">>, GroupId},
                    {<<"query_type">>, <<"add_member">>}],
                children = []}]};
        _ ->
            case odbc_groupchat:add_members(LServer, GroupId, NewMembers) of
                {ok, MembersResult} ->
                    case odbc_groupchat:get_groupinfo_by_groupid(LServer, GroupId) of
                        {ok, #group{groupid = GroupId, master = GroupOwner, groupname = GroupName}} ->
                            push_groupmember(GroupId, GroupName, GroupOwner, LServer,
                                ExistsMembers ++ NewMembers, MembersResult, <<"add">>),
                            IQ#iq{type = result,
                                sub_el = [SubEl#xmlel{attrs =
                                [{<<"xmlns">>, ?NS_GROUPCHAT},
                                    {<<"groupid">>, GroupId},
                                    {<<"query_type">>, <<"add_member">>}],
                                    children = [{xmlcdata,
                                        members_to_json(MembersResult)}]}]};
                        {error, _} ->
                            make_error_reply(IQ, <<"15102">>)
                    end;
                {error, _} ->
                    make_error_reply(IQ, <<"15104">>)
            end
    end.

do_remove_members(#iq{sub_el = SubEl} = IQ, LServer, GroupId, MembersList) ->
    case odbc_groupchat:remove_members(LServer, GroupId, MembersList) of
        {ok, MembersInfoList} ->
            case odbc_groupchat:get_members_by_groupid(LServer, GroupId) of
                {ok, RemainMembers} ->
                    RemainJid = [Jid || {Jid, _} <- RemainMembers],
                    push_groupmember(GroupId, <<>>, <<>>, LServer, RemainJid ++ MembersList,
                        MembersInfoList, <<"remove">>);
                _ ->
                    push_groupmember(GroupId, <<>>, <<>>, LServer, MembersList,
                        MembersInfoList, <<"remove">>)
            end,
            IQ#iq{type = result, sub_el = [SubEl]};
        _ ->
            make_error_reply(IQ, <<"15102">>)
    end.

push_groupmember(GroupId, GroupName, GroupOwner, Server, ToList, MembersInfoList, Action) ->
    Contents = groupmember_json(MembersInfoList, Action),
    PreAttrs = [{<<"xmlns">>, ?NS_GROUPCHAT}, {<<"type">>, <<"groupmember">>},
        {<<"groupid">>, GroupId}, {<<"groupname">>, GroupName}],
    Attrs = case GroupOwner of
                <<>> -> PreAttrs;
                _ -> [{<<"master">>, GroupOwner} | PreAttrs]
            end,
    push(GroupId, Server, Attrs, Contents, ToList).

push_groupinfo(Group, Server, ToList, Action) ->
    Contents = groupinfo_json(Group, Action),
    Attrs = [{<<"xmlns">>, ?NS_GROUPCHAT}, {<<"type">>, <<"groupinfo">>}],
    push(Group#group.groupid, Server, Attrs, Contents, ToList).

push(GroupId, Server, Attrs, Contents, ToList) ->
    From = jlib:jid_to_binary({GroupId, Server, <<>>}),
    LangAttr = {<<"xml:lang">>, <<"en">>},
    FromAttr = {<<"from">>, From},
    TypeAttr = {<<"type">>, <<"chat">>},
    Packet = {xmlel, <<"message">>, [],
        [{xmlel, <<"push">>, Attrs,
            [{xmlcdata, Contents}]}
        ]},
    FromJid = jlib:make_jid(GroupId, Server, <<>>),
    lists:foreach(fun(ToJid) ->
        ToAttr = {<<"to">>, ToJid},
        ejabberd_router:route(FromJid, jlib:binary_to_jid(ToJid),
            Packet#xmlel{attrs = [FromAttr, ToAttr, TypeAttr, LangAttr]}) end,
        ToList).

-spec group_to_json(#group{}) -> binary().
group_to_json(Group) ->
    iolist_to_binary(mochijson2:encode({struct, record_to_json(Group)})).

-spec grouplist_to_json([#group{}]) -> binary().
grouplist_to_json(List) ->
    JsonArray = [{struct, record_to_json(Group)} || Group <- List],
    iolist_to_binary(mochijson2:encode(JsonArray)).

groupinfo_json(Group, Action) ->
    JsonArray = [{struct, [{action, Action} | record_to_json(Group)]}],
    iolist_to_binary(mochijson2:encode(JsonArray)).

groupmember_json(MembersInfoList, Action) ->
    JsonArray = [{struct, [{userjid, Jid}, {nickname, NickName},
        {action, Action}]} || {Jid, NickName} <- MembersInfoList],
    iolist_to_binary(mochijson2:encode(JsonArray)).

members_to_json(Members) ->
    JsonArray = [{struct, [{userjid, UserJid}, {nickname, NickName}]} || {UserJid, NickName} <- Members],
    iolist_to_binary(mochijson2:encode(JsonArray)).

record_to_json(Group) ->
    [_ | L] = tuple_to_list(Group),
    lists:filter(fun(X) ->
        case X of
            {_, undefined} -> false;
            {_, null} -> false;
            _ -> true
        end
    end,
        lists:zip(record_info(fields, group), L)
    ).

get_group_type(SubEl) ->
    case xml:get_tag_attr_s(<<"grouptype">>, SubEl) of
        <<>> ->
            ?NORAML_GROUP;
        R ->
            R
    end.

-spec make_error_reply(#iq{}, binary()) -> #iq{}.
make_error_reply(#iq{sub_el = SubEl} = IQ, Code) ->
    Err = #xmlel{name = <<"error">>,
        attrs = [{<<"code">>, <<"500">>}, {<<"type">>, <<"cancel">>}],
        children = [
            #xmlel{name = <<"undefined-condition">>,
                attrs = [{<<"xmlns">>, <<"aft:error">>}]
            },
            #xmlel{name = <<"code">>
                , attrs = [{<<"xmlns">>, ?NS_AFT_ERROR}]
                , children = [#xmlcdata{content = Code}]
            }
        ]
    },
    IQ#iq{type = error, sub_el = [SubEl, Err]}.

