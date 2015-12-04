-module(mod_mms).

-behaviour(gen_mod).

-export([start/2, stop/1, process_iq/3]).

-export([get_url/2]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_mms.hrl").

-define(NS_MMS, <<"aft:mms">>).

start(Host, _Opts) ->
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host,
        ?NS_MMS, ?MODULE, process_iq, no_queue).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_MMS).


%% -----------------------------------------------
%% IQ Handler
%% -----------------------------------------------

process_iq(From, To, #iq{xmlns = ?NS_MMS, type = _Type, sub_el = SubEl} = IQ) ->
    case SubEl of
        #xmlel{name = <<"query">>} ->
            case xml:get_tag_attr_s(<<"query_type">>, SubEl) of
                <<"download">> ->
                    download(From, To, IQ);
                <<"upload">> ->
                    upload(From, To, IQ);
                <<"complete">> ->
                    multi_complete(From, To, IQ);
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
            end;
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;
process_iq(_, _, IQ) ->
    #iq{sub_el = SubEl} = IQ,
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.


%% -----------------------------------------------
%% Internal functions
%% -----------------------------------------------

download(#jid{lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    case get_fileid(SubEl) of
        undefined -> make_error_reply(IQ, <<"19001">>);
        FileId ->
            case get_file(LServer, FileId) of
                #mms_file{uid = Uid, type = Type} ->
                    case Type of
                        ?PROJECT -> % download from project library
                            make_error_reply(IQ, <<"19002">>);
                        _ ->
                            case get_url(Uid, Type) of
                                error ->
                                    make_error_reply(IQ, <<"19003">>);
                                Url ->
                                    IQ#iq{type = result, sub_el = [
                                        SubEl#xmlel{children = [{xmlcdata, Url}]}]}
                            end
                    end;
                _ ->
                    make_error_reply(IQ, <<"19004">>)
            end
    end.

upload(#jid{lserver = LServer, luser = LUser} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    E = integer_to_binary(generate_expiration()),
    S = list_to_binary(?MMS_SECRET),
    Jid = <<LUser/binary, $@, LServer/binary>>,
    Time = integer_to_binary(stamp_now()),
    case get_fileid(SubEl) of
        undefined ->
            FileType = case xml:get_tag_attr_s(<<"type">>, SubEl) of
                           ?AVATAR ->
                               ?AVATAR;
                           ?PROJECT ->
                               ?PROJECT;
                           _ ->
                               ?MESSAGE
                       end,
            F = generate_fileid(),
            T = generate_token(Jid, F, E, S, FileType),
            Uid = generate_fileid(),
            case xml:get_tag_attr_s(<<"multipart">>, SubEl) of
                <<"1">> ->
                    case mod_mms_s3:multi_init(#mms_file{uid = Uid, type = FileType}) of
                        {ok, UploadId} ->
                            case init_multiparts(LServer, F, Uid, UploadId, FileType) of
                                ok ->
                                    case ejabberd_redis:cmd(["SET", <<"upload:", F/binary>>,
                                        <<"1:", FileType/binary, $:, Time/binary>>]) of
                                        <<"OK">> ->
                                            make_reply(T, F, E, UploadId, IQ);
                                        _ ->
                                            make_error_reply(IQ, <<"19005">>)
                                    end;
                                _ ->
                                    make_error_reply(IQ, <<"19014">>)
                            end;
                        {error, _} ->
                            make_error_reply(IQ, <<"19007">>)
                    end;
                _ ->
                    case ejabberd_redis:cmd(["SET", <<"upload:", F/binary>>,
                        <<"0:", FileType/binary, $:, Time/binary>>]) of
                        <<"OK">> ->
                            make_reply(T, F, E, undefined, IQ);
                        _ ->
                            make_error_reply(IQ, <<"19005">>)
                    end
            end;
        F ->
            case ejabberd_redis:cmd(["GET", <<"upload:", F/binary>>]) of
                undefined ->
                    make_error_reply(IQ, <<"19006">>);
                <<IsMultipart:1/binary, $:, Type:1/binary, $:, _/binary>> ->
                    T = generate_token(Jid, F, E, S, Type),
                    ejabberd_redis:cmd(["SET", <<"upload:", F/binary>>,
                        <<IsMultipart/binary, $:, Type/binary, $:, Time/binary>>]),
                    make_reply(T, F, E, undefined, IQ);
                _ ->
                    make_error_reply(IQ, <<"19006">>)
            end
    end.

multi_complete(#jid{lserver = LServer, luser = LUser} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    Jid = <<LUser/binary, $@, LServer/binary>>,
    case {get_tag(SubEl, <<"file">>), get_tag(SubEl, <<"uploadid">>)} of
        {undefined, _} ->
            make_error_reply(IQ, <<"19008">>);
        {_, undefined} ->
            make_error_reply(IQ, <<"19009">>);
        {FileId, UploadId} ->
            case get_multipart_uid(LServer, FileId, UploadId) of
                {error, _} ->
                    make_error_reply(IQ, <<"19011">>);
                {Uid, Type} ->
                    case get_multiparts(LServer, UploadId) of
                        {error, _} ->
                            make_error_reply(IQ, <<"19012">>);
                        {ok, ETags} ->
                            case mod_mms_s3:multi_complete(#mms_file{uid = Uid, type = Type}, UploadId, ETags) of
                                ok ->
                                    File = #mms_file{id = FileId, uid = Uid, type = Type, owner = Jid, filename = <<>>},
                                    complete_multiparts(LServer, UploadId, File),
                                    ejabberd_redis:cmd(["DEL", <<"upload:", FileId/binary>>]),
                                    IQ#iq{type = result};
                                {error, _} ->
                                    make_error_reply(IQ, <<"19013">>)
                            end
                    end
            end
    end.

%% ===============================
%% helper
%% ===============================

-spec make_error_reply(#iq{}, binary()) -> #iq{}.
make_error_reply(#iq{sub_el = SubEl} = IQ, Code) ->
    Error = #xmlel{name = <<"error">>, attrs = [{<<"code">>, Code}]},
    IQ#iq{type = error, sub_el = [SubEl, Error]}.

make_reply(T, F, E, U, #iq{sub_el = SubEl} = IQ) ->
    Token = #xmlel{
        name = <<"token">>,
        children = [{xmlcdata, T}]},
    File = #xmlel{
        name = <<"file">>,
        children = [{xmlcdata, F}]},
    Expiration = #xmlel{
        name = <<"expiration">>,
        children = [{xmlcdata, E}]},
    El = [Token, File, Expiration],
    El2 = case U of
              undefined -> El;
              _ ->
                  UploadId = #xmlel{
                      name = <<"uploadid">>,
                      children = [{xmlcdata, U}]},
                  [UploadId | El]
          end,
    NEl = SubEl#xmlel{children = El2},
    IQ#iq{type = result, sub_el = [NEl]}.

get_fileid(SubEl) ->
    case xml:get_subtag(SubEl, <<"file">>) of
        false ->
            undefined;
        FileEl ->
            case xml:get_tag_cdata(FileEl) of
                <<>> ->
                    undefined;
                FileUid ->
                    FileUid
            end
    end.

get_tag(SubEl, Tag) ->
    case xml:get_subtag(SubEl, Tag) of
        false ->
            undefined;
        El ->
            case xml:get_tag_cdata(El) of
                <<>> ->
                    undefined;
                Data ->
                    Data
            end
    end.

-spec get_url(binary(), binary()) -> binary() | error.
get_url(Uid, Type) ->
    mod_mms_s3:get(#mms_file{uid = Uid, type = Type}, 30 * 60).

-spec generate_token(binary(), binary(), binary(), binary(), binary()) -> string().
generate_token(Jid, FileId, Expiration, Secret, Private) ->
    md5(<<Jid/binary, FileId/binary, Expiration/binary, Secret/binary, Private/binary>>).

-spec generate_expiration() -> integer().
generate_expiration() ->
    stamp_now() + 30 * 60. % 30 minutes expiration for upload token

stamp_now() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.

-spec generate_fileid() -> binary().
generate_fileid() ->
    uuid:generate().

-spec md5(string() | binary()) -> string().
md5(Text) ->
    lists:flatten([io_lib:format("~.16b", [N]) || N <- binary_to_list(erlang:md5(Text))]).

%% ===========================
%% odbc
%% ===========================

-spec get_file(binary(), binary()) -> #mms_file{} | {error, _}.
get_file(LServer, Id) ->
    Query = [<<"select uid,filename,owner,type,UNIX_timestamp(created_at) from mms_file where id= '">>, Id, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, []} ->
            {error, not_exists};
        {selected, _, [{Uid, FileName, Owner, Type, CreatedAt}]} ->
            #mms_file{id = Id, uid = Uid, filename = FileName, owner = Owner,
                type = Type, created_at = CreatedAt};
        Reason ->
            {error, Reason}
    end.

init_multiparts(LServer, FileId, Uid, UploadId, Type) ->
    Query = [<<"insert into mms_multipart(upload_id,fileid,uid,type) values('">>,
        UploadId, "','", FileId, "','", Uid, "','", Type, "');"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {updated, 1} ->
            ok;
        Reason ->
            {error, Reason}
    end.

-spec get_multipart_uid(binary(), binary(), binary()) -> {binary(), binary()} | {error, _}.
get_multipart_uid(LServer, FileId, UploadId) ->
    Query = [<<"select uid,type from mms_multipart where upload_id = '">>,
        UploadId, <<"' and fileid = '">>, FileId, <<"';">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, []} ->
            {error, not_exists};
        {selected, _, [{Uid, Type}]} ->
            {Uid, Type};
        Reason ->
            {error, Reason}
    end.

-spec get_multiparts(binary(), binary()) -> {ok, list()} | {error, _}.
get_multiparts(LServer, UploadId) ->
    Query = [<<"select part_number,etag from mms_multipart_records where upload_id = '">>, UploadId, <<"';">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, []} ->
            {error, not_exists};
        {selected, _, R} ->
            {ok, R};
        Reason ->
            {error, Reason}
    end.

-spec complete_multiparts(binary(), binary(), #mms_file{}) -> ok | {error, _}.
complete_multiparts(LServer, UploadId, File) ->
    F = fun() ->
        Query1 = [<<"delete from mms_multipart where upload_id = '">>,
            UploadId, <<"';">>],
        Query2 = [<<"delete from mms_multipart_records where upload_id = '">>, UploadId, <<"';">>],
        ejabberd_odbc:sql_query_t(Query1),
        ejabberd_odbc:sql_query_t(Query2),
        Query = [<<"insert into mms_file(id,uid,filename,owner,type,created_at) values('">>,
            File#mms_file.id, "','", File#mms_file.uid, "','", File#mms_file.filename, "', '",
            File#mms_file.owner, "',", File#mms_file.type, <<", now());">>],
        ejabberd_odbc:sql_query_t(Query)
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {updated, 1}} ->
            ok;
        Reason ->
            {error, Reason}
    end.