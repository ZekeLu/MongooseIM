%%%===================================================================
%%% @doc An extension to XEP-0077 (http://xmpp.org/extensions/xep-0077.html)
%%%      Allow users to register account with telephone number or email.
%%%      It's based on mod_register.erl with these changes:
%%%         1. remove ip checking
%%%         2. remove acl checking
%%%         3. don't support get IQ
%%%         4. require activation of new account (by SMS or email)
%%%===================================================================

-module(mod_register_aft).

-behaviour(gen_mod).

%% gen_mod callbacks
-export([start/2,
         stop/1,
         clean_opts/1]).

%% hooks
-export([stream_feature_register/2,
         unauthenticated_iq_register/4]).

%% iq handler
-export([process_iq/3, process_iq2/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").


%%%===================================================================
%%% gen_mod callbacks
%%%===================================================================

                                                % process_iq: handle change password iq.
                                                % process_iq2: handle all other our custom iq.
                                                % unauthenticated_iq_register: handle our custom unauthenticated iq

start(Host, Opts) ->
    IQDisc = gen_mod:get_opt(iqdisc, Opts, one_queue),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_AFT_INFORMATION,
                                  ?MODULE, process_iq2, no_queue),
    gen_iq_handler:add_iq_handler(ejabberd_local, Host, ?NS_AFT_REGISTER,
                                  ?MODULE, process_iq, IQDisc),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_AFT_REGISTER,
                                  ?MODULE, process_iq, IQDisc),
    ejabberd_hooks:add(c2s_stream_features, Host,
                       ?MODULE, stream_feature_register, 50),
    ejabberd_hooks:add(c2s_unauthenticated_iq, Host,
                       ?MODULE, unauthenticated_iq_register, 50),
    mnesia:create_table(mod_register_aft_ip,
                        [{ram_copies, [node()]},
                         {local_content, true},
                         {attributes, [key, value]}]),
    mnesia:add_table_copy(mod_register_aft_ip, node(), ram_copies),
    ok.

stop(Host) ->
    ejabberd_hooks:delete(c2s_stream_features, Host,
                          ?MODULE, stream_feature_register, 50),
    ejabberd_hooks:delete(c2s_unauthenticated_iq, Host,
                          ?MODULE, unauthenticated_iq_register, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_local, Host, ?NS_AFT_REGISTER),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_AFT_REGISTER),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_AFT_INFORMATION).

clean_opts(Opts) ->
    lists:map(fun clean_opt/1, Opts).

clean_opt({registration_watchers, Watchers}) ->
    CleanWatchers = lists:map(fun ejabberd_binary:string_to_binary/1, Watchers),
    {registration_watchers, CleanWatchers};
clean_opt(Item) ->
    Item.

%%%===================================================================
%%% hooks
%%%===================================================================

stream_feature_register(Acc, _Host) ->
    [#xmlel{name = <<"aft-register">>,
            attrs = [{<<"xmlns">>, ?NS_AFT_FEATURE_IQREGISTER}]} | Acc].

unauthenticated_iq_register(_Acc,
                            Server, #iq{xmlns = ?NS_AFT_REGISTER} = IQ, IP) ->
    Address = case IP of
                  {A, _Port} -> A;
                  _ -> undefined
              end,
    ResIQ = process_unauthenticated_iq(Server, IQ, Address),
    Res1 = jlib:replace_from_to(jlib:make_jid(<<>>, Server, <<>>),
                                jlib:make_jid(<<>>, <<>>, <<>>),
                                jlib:iq_to_xml(ResIQ)),
    jlib:remove_attr(<<"to">>, Res1);
unauthenticated_iq_register(Acc, _Server, _IQ, _IP) ->
    Acc.

%%%===================================================================
%%% iq handler
%%%===================================================================

process_iq2(#jid{user = User, lserver = Server, lresource = _Resource} = _From,
            #jid{lserver = Server} = _To,
            #iq{type = _Type, lang = _Lang1, sub_el = SubEl} = IQ) ->
    PasswordTag = xml:get_subtag(SubEl, <<"password">>),
    TokenTag = xml:get_subtag(SubEl, <<"token">>),
    CodeTag = xml:get_subtag(SubEl, <<"code">>),
    PhoneTag = xml:get_subtag(SubEl, <<"phone">>),
    Type = xml:get_subtag(SubEl, <<"type">>),
    SubType = xml:get_tag_attr(<<"subtype">>, SubEl),

    case check_iq_do(SubType, PasswordTag, PhoneTag, TokenTag, CodeTag, Type) of
        {error, bad_request} ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]};
        {ok, <<"verify_password">>} ->
            case varify_password(User, Server, xml:get_tag_cdata(PasswordTag)) of
                {error, Error} ->
                    IQ#iq{type = error,
                          sub_el = [SubEl, Error]};
                {ok, Result} ->
                    IQ#iq{type = result,
                          sub_el = [#xmlel{name = <<"query">>,
                                           attrs = [{<<"xmlns">>, ?NS_AFT_INFORMATION},
                                                    {<<"subtype">>, <<"verify_password">>}],
                                           children = [#xmlel{name = <<"token">>,
                                                              children = [#xmlcdata{content = Result}]}]}]}

            end;
        {ok, <<"get_code">>} ->
            case change_phone(User, Server, xml:get_tag_cdata(PhoneTag),
                              xml:get_tag_cdata(TokenTag)) of
                {error, Error} ->
                    IQ#iq{type = error, sub_el = [SubEl, Error]};
                {ok, Result} ->
                    IQ#iq{type = result,
                          sub_el = [#xmlel{name = <<"query">>,
                                           attrs = [{<<"xmlns">>, ?NS_AFT_INFORMATION},
                                                    {<<"subtype">>, <<"get_code">>}],
                                           children = [#xmlel{name = <<"phone">>,
                                                              children = [#xmlcdata{content = Result}]}]}]}
            end;
        {ok, <<"verify_code">>} ->
            case verify_change_phone(User, Server, xml:get_tag_cdata(PhoneTag),
                                     xml:get_tag_cdata(CodeTag)) of
                {error, Error} ->
                    IQ#iq{type = error, sub_el = [SubEl, Error]};
                {ok, Result} ->
                    IQ#iq{type = result,
                          sub_el = [#xmlel{name = <<"query">>,
                                           attrs = [{<<"xmlns">>, ?NS_AFT_INFORMATION},
                                                    {<<"subtype">>, <<"verify_code">>}],
                                           children = [#xmlel{name = <<"phone">>,
                                                              children = [#xmlcdata{content = Result}]}]}]}
            end;
        {ok, <<"invite">>} ->
            case invite(User, Server, xml:get_tag_cdata(PhoneTag),
                        xml:get_tag_cdata(Type)) of
                {error, Error} ->
                    IQ#iq{type = error, sub_el = [SubEl, Error]};
                ok ->
                    IQ#iq{type = result}
            end;
        true ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;
process_iq2(_From, _To, #iq{sub_el = SubEl} = IQ) ->
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.

process_iq(#jid{user = User, lserver = Server, lresource = Resource} = _From,
           #jid{lserver = Server} = _To,
           #iq{type = set, lang = Lang1, sub_el = SubEl} = IQ) ->
    Lang = binary_to_list(Lang1),
    RemoveTag = xml:get_subtag(SubEl, <<"remove">>),
    OldPasswordTag = xml:get_subtag(SubEl, <<"old_password">>),
    PasswordTag = xml:get_subtag(SubEl, <<"password">>),

    if
        (RemoveTag /= false) and (OldPasswordTag =:= false) and (PasswordTag =:= false) ->
            ResIQ = IQ#iq{type = result, sub_el = [SubEl]},
            ejabberd_router:route(
              jlib:make_jid(User, Server, Resource),
              jlib:make_jid(User, Server, Resource),
              jlib:iq_to_xml(ResIQ)),
            ejabberd_auth:remove_user(User, Server),
            ignore;
        (RemoveTag =:= false) and (OldPasswordTag /= false) and (PasswordTag /= false) ->
            try_set_password(User, Server, xml:get_tag_cdata(OldPasswordTag),
                             xml:get_tag_cdata(PasswordTag), IQ, SubEl, Lang);
        true ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;
process_iq(_From, _To, #iq{sub_el = SubEl} = IQ) ->
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.


%%%===================================================================
%%% Internal functions. called by unauthenticated_iq_register or IQ handler.
%%%===================================================================

check_do_what(SubType, PhoneTag, CodeTag, NickTag, PasswordTag, TokenTag) ->
    case {SubType, PhoneTag, CodeTag, NickTag, PasswordTag, TokenTag} of
        {{value, <<"reg_get_code">>}, Phone, false, false, false, false} ->
            if
                Phone /= false ->
                    {ok, <<"reg_get_code">>};
               true ->
                    {error, bad_request}
            end;
        {{value, <<"find_get_code">>}, Phone, false, false, false, false} ->
            if
                Phone /= false ->
                    {ok, <<"find_get_code">>};
                true ->
                    {error, bad_request}
            end;
        {{value, <<"register">>}, Phone, Code, Nick, false, false} ->
            if
                (Phone /= false) and (Code /= false) and (Nick /= false)  ->
                    {ok, <<"register">>};
               true ->
                    {error, bad_request}
            end;
        {{value, <<"find">>}, Phone, Code, false, false, false} ->
            if
                (Phone /= false) and (Code /= false)  ->
                    {ok, <<"find">>};
               true ->
                    {error, bad_request}
            end;
        {{value, <<"set_password">>}, false, false, false, Password, Token} ->
            if
                (Password /= false) and (Token /= false) ->
                    {ok, <<"set_password">>};
               true ->
                    {error, bad_request}
            end;
        _ ->
            {error, bad_request}
    end.

process_unauthenticated_iq(Server,
                           #iq{ lang = Lang1, sub_el = SubEl} = IQ,
                           IpAddress) ->
    Lang = binary_to_list(Lang1),
    PhoneTag = xml:get_subtag(SubEl, <<"phone">>),
    CodeTag = xml:get_subtag(SubEl, <<"code">>),
    NickTag = xml:get_subtag(SubEl, <<"nick">>),
    PasswordTag = xml:get_subtag(SubEl, <<"password">>),
    TokenTag = xml:get_subtag(SubEl, <<"token">>),
    SubType = xml:get_tag_attr(<<"subtype">>, SubEl),

    case check_do_what(SubType, PhoneTag, CodeTag, NickTag, PasswordTag, TokenTag) of
        {error, bad_request} ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]};
        {ok, <<"reg_get_code">>} ->
            Phone = get_tag_cdata(PhoneTag),
            case ejabberd_auth_odbc:user_info(Server, Phone) of
                {info, _} ->
                    IQ#iq{type = error, sub_el = [SubEl, ?AFT_ERR_PHONE_EXIST]};
                not_exist ->
                    %%case get_code(Phone, 600, 60, <<"code_">>, <<"74663">>) of
                    case get_code(Phone, 600, 60, <<"code_">>) of
                        {ok, _Code} ->
                            IQ#iq{type = result,
                                sub_el = [#xmlel{name = <<"query">>,
                                    attrs = [{<<"xmlns">>, <<"aft:register">>},
                                        {<<"subtype">>, <<"reg_get_code">>}],
                                    children = [#xmlel{name = <<"phone">>,
                                        children = [#xmlcdata{content = Phone}]}]}]};
                        {error, Error} ->
                            IQ#iq{type = error, sub_el = [SubEl, Error]}
                    end;
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_INTERNAL_SERVER_ERROR]}
            end;
        {ok, <<"find_get_code">>} ->
            Phone = get_tag_cdata(PhoneTag),
            case ejabberd_auth_odbc:user_info(Server, Phone) of
                {info, _} ->
                    %%case get_code(Phone, 600, 60, <<"code_">>, <<"74663">>) of
                    case get_code(Phone, 600, 60, <<"code_">>) of
                        {ok, Code} ->
                            IQ#iq{type = result,
                                sub_el = [#xmlel{name = <<"query">>,
                                    attrs = [{<<"xmlns">>, <<"aft:register">>},
                                        {<<"subtype">>, <<"find_get_code">>}],
                                    children = [#xmlel{name = <<"phone">>,
                                        children = [#xmlcdata{content = Phone}]},
                                        #xmlel{name = <<"code">>,
                                            children = [#xmlcdata{content = Code}]}]}]};
                        {error, Error} ->
                            IQ#iq{type = error, sub_el = [SubEl, Error]}
                    end;
                not_exist ->
                    IQ#iq{type = error, sub_el = [SubEl, ?AFT_ERR_PHONE_NOT_EXIST]};
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_INTERNAL_SERVER_ERROR]}
            end;
        {ok, <<"register">>} ->
            case try_register(get_tag_cdata(PhoneTag),
                              get_tag_cdata(NickTag),
                              get_tag_cdata(CodeTag),
                              Server, Lang, IpAddress) of
                {ok, {Phone, Token}} ->
                    IQ#iq{type = result,
                          sub_el = [#xmlel{name = <<"query">>,
                                           attrs = [{<<"xmlns">>, <<"aft:register">>},
                                                    {<<"subtype">>, <<"register">>}],
                                           children = [#xmlel{name = <<"phone">>,
                                                              children = [#xmlcdata{content = Phone}]},
                                                       #xmlel{name = <<"token">>,
                                                              children = [#xmlcdata{content = Token}]}]}]};
                {error, Error} ->
                    IQ#iq{type = error,
                          sub_el = [SubEl, Error]}
            end;
        {ok, <<"find">>} ->
            case find(get_tag_cdata(PhoneTag),
                      get_tag_cdata(CodeTag),
                      Server, Lang, IpAddress) of
                {ok, {Phone, Token}} ->
                    IQ#iq{type = result,
                          sub_el = [#xmlel{name = <<"query">>,
                                           attrs = [{<<"xmlns">>, <<"aft:register">>},
                                                    {<<"subtype">>, <<"find">>}],
                                           children = [#xmlel{name = <<"phone">>,
                                                              children = [#xmlcdata{content = Phone}]},
                                                       #xmlel{name = <<"token">>,
                                                              children = [#xmlcdata{content = Token}]}]}]};
                {error, Error} ->
                    IQ#iq{type = error,
                          sub_el = [SubEl, Error]}
            end;
        {ok, <<"set_password">>} ->
            case try_set_password(get_tag_cdata(TokenTag),
                                  get_tag_cdata(PasswordTag), Server) of
                {ok, {ok, Phone}} ->
                    Pas = get_tag_cdata(PasswordTag),
                    IQ#iq{type = result,
                          sub_el = [#xmlel{name = <<"query">>,
                                           attrs = [{<<"xmlns">>, <<"aft:register">>},
                                                    {<<"subtype">>, <<"set_password">>}],
                                           children = [#xmlel{name = <<"phone">>,
                                                              children = [#xmlcdata{content = Phone}]},
                                                       #xmlel{name = <<"password">>,
                                                              children = [#xmlcdata{content = Pas}]}]}]};
                {error, Error} ->
                    IQ#iq{type = error,
                          sub_el = [SubEl, Error]}
            end
    end.

try_register(Phone, Nick, Code, Server, _Lang, IpAddress) ->
    case check_nick_name_valid(Nick) of
        true ->
            case ejabberd_redis:cmd(["MGET", [<<"code_", Phone/binary>>]]) of
                [undefined] ->
                    {error, ?AFT_ERR_BAD_CODE};
                [CacheCode] ->
                    if CacheCode =:= Code ->
                            Temp = generate_token(),
                            Token = <<"setpwd_", Temp/binary>>,
                            case ejabberd_auth_odbc:user_info(Server, Phone) of
                                {error, Error} ->
                                    ?ERROR_MSG("try_register error=~p~n", [Error]),
                                    {error, ?ERR_INTERNAL_SERVER_ERROR};
                                {info, _} ->
                                    ejabberd_redis:cmd(["DEL", [<<"code_", Phone/binary>>]]),
                                    {error, ?AFT_ERR_PHONE_EXIST};
                                not_exist ->
                                    ejabberd_redis:cmd(["DEL", [<<"code_", Phone/binary>>]]),
                                    GUID = generate_jid(),
                                    case check_timeout(IpAddress) of
                                        true ->
                                            case ejabberd_auth:aft_try_register(GUID, Server, Phone, Nick) of
                                                {error, not_allowed} ->
                                                    remove_timeout(IpAddress),
                                                    {error, ?AFT_ERR_IP_FORBIDDEN};
                                                {atomic, exists} ->
                                                    remove_timeout(IpAddress),
                                                    {error, ?AFT_ERR_PHONE_EXIST};
                                                {atomic, ok} ->
                                                    SurvivalTime = 3600,
                                                    ejabberd_redis:cmd([["DEL", [Token]],
                                                                        ["APPEND", [Token], [Phone, <<":">>, GUID]],
                                                                        ["EXPIRE", [Token], SurvivalTime]]),
                                                    JID = jlib:make_jid(GUID, Server, <<>>),
                                                %   send_registration_notifications(JID, IpAddress),
                                                    send_welcome_message(JID),
                                                    {ok, {Phone, Token}};
                                                _ ->
                                                    remove_timeout(IpAddress),
                                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                                            end;
                                        false ->
                                            {error, ?AFT_ERR_REGISTER_SO_QUICKLY}
                                    end
                            end;
                       true ->

                            {error, ?AFT_ERR_BAD_CODE}
                    end
            end;
        false ->
            {error, ?AFT_ERR_BAD_NICK_FORMAT}
    end.

find(Phone, Code, Server, _Lang, _IpAddress) ->
    case ejabberd_redis:cmd(["MGET", [<<"code_", Phone/binary>>]]) of
        [undefined] ->
            {error, ?AFT_ERR_BAD_CODE};
        [CacheCode] ->
            if CacheCode =:= Code ->
                    case ejabberd_auth_odbc:user_info(Server, Phone) of
                        {info, {UserName, _, _}} ->
                            Temp = generate_token(),
                            Token = <<"setpwd_", Temp/binary >>,
                            SurvivalTime = 3600,
                            ejabberd_redis:cmd([["DEL", [Token]],
                                                ["APPEND", [Token], [Phone, <<":">>, UserName]],
                                                ["EXPIRE", [Token], SurvivalTime]]),
                            {ok, {Phone,Token}};
                        not_exist ->
                            {error, ?AFT_ERR_PHONE_NOT_EXIST};
                        {error, Error} ->
                            ?ERROR_MSG("find error=~p~n", [Error]),
                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                    end;
               true ->
                    {error, ?AFT_ERR_BAD_CODE}
            end
    end.

try_set_password(Token, Password, Server) ->
    case ejabberd_redis:cmd(["MGET", [Token]]) of
        [undefined] ->
            {error, ?AFT_ERR_PASSWORD_SETTING_EXPIRE};
        [CacheData] ->
            case binary:split(CacheData, <<":">>, [global]) of
                [Phone, User] ->
                    case ejabberd_auth_odbc:user_info(Server, Phone) of
                        {info, _} ->
                            case check_password(Server, Password) of
                                {ok, ok} ->
                                    ejabberd_redis:cmd(["DEL", [Token]]),
                                    case ejabberd_auth:set_password(User, Server, Password) of
                                        ok ->
                                            {ok, {ok, Phone}};
                                        _ ->
                                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                                    end;
                                {error, invalid_password} ->
                                    {error, ?AFT_ERR_BAD_PASSWORD_FORMAT};
                                {error, error} ->
                                    {error, ?AFT_ERR_BAD_PASSWORD_FORMAT};
                                {error, weak_password} ->
                                    {error, ?AFT_ERR_WEAK_PASSWORD}
                            end;
                        not_exist ->
                            {error, ?AFT_ERR_PHONE_NOT_EXIST};
                        _ ->
                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                    end;
                _ ->
                    {error, ?AFT_ERR_LOGIC_SERVER}
            end;
        _ ->
            {error, ?AFT_ERR_LOGIC_SERVER}
    end.

%% @doc Try to change password and return IQ response.
try_set_password(User, Server, OldPassword, Password, IQ, SubEl, Lang) ->
    case ejabberd_auth:check_password(User, Server, OldPassword) of
        true ->
            case is_strong_password(Server, Password) of
                true ->
                    case ejabberd_auth:set_password(User, Server, Password) of
                        ok ->
                            IQ#iq{type = result, sub_el = [SubEl]};
                        {error, empty_password} ->
                            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]};
                        {error, not_allowed} ->
                            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]};
                        {error, invalid_jid} ->
                            IQ#iq{type = error, sub_el = [SubEl, ?ERR_ITEM_NOT_FOUND]};
                        _ ->
                            IQ#iq{type = error, sub_el = [SubEl, ?ERR_INTERNAL_SERVER_ERROR]}
                    end;
                false ->
                    ErrText = "The password is too weak",
                    IQ#iq{type = error,
                          sub_el = [SubEl, ?ERRT_NOT_ACCEPTABLE(Lang, ErrText)]}
            end;
        _ ->
            ErrText = "The old password is wrong",
            IQ#iq{type = error,
                  sub_el = [SubEl, ?ERRT_NOT_AUTHORIZED(Lang, ErrText)]}
    end.

check_iq_do(SubType, PasswordTag, PhoneTag, TokenTag, CodeTag, Type) ->
    case {SubType, PasswordTag, PhoneTag, TokenTag, CodeTag, Type} of
        {{value, <<"verify_password">>}, P, false, false, false, false} ->
            if P /= false ->
                {ok, <<"verify_password">>};
                true ->
                    {error, bad_request}
            end;
        {{value, <<"get_code">>}, false, P, T, false, false} ->
            if (P /= false) and (T /= false) ->
                {ok, <<"get_code">>};
                true ->
                    {error, bad_request}
            end;
        {{value, <<"verify_code">>}, false, P, false, C, false} ->
            if (P /= false) and (C /= false) ->
                {ok, <<"verify_code">>};
                true ->
                    {error, bad_request}
            end;
        {{value, <<"invite">>}, false, P, false, false, T} ->
            if (P /= false) and (T /= false) ->
                {ok, <<"invite">>};
                true ->
                    {error, bad_request}
            end;
        _ ->
            {error, bad_request}
    end.

varify_password(User, Server, Password) ->
    case ejabberd_auth:check_password(User, Server, Password) of
        true ->
            Token = generate_token(),
            TokenKey = <<"varify_token_", User/binary>>,
            SurvivalTime = 3600,
            ejabberd_redis:cmd([["DEL", [TokenKey]],
                ["APPEND", [TokenKey], [Token]],
                ["EXPIRE", [TokenKey], SurvivalTime]]),
            {ok, Token};
        _ ->
            {error, ?AFT_ERR_BAD_PASSWORD}
    end.

change_phone(User, Server, Phone, Token) ->
    case check_phone_number_valid(Phone) of
        true ->
            case ejabberd_redis:cmd(["MGET", [<<"varify_token_", User/binary>>]]) of
                [undefined] ->
                    {error, ?AFT_ERR_BAD_CODE};
                [CacheToken] ->
                    if
                        CacheToken =:= Token ->
                            case ejabberd_auth_odbc:user_info(Server, Phone) of
                                {info, {UserName, _, _}} ->
                                    if UserName =:= User -> {error, ?ERR_BAD_REQUEST};
                                        true -> {error, ?AFT_ERR_PHONE_EXIST}
                                    end;
                                not_exist ->
                                    %%get_code(Phone, 600, 60, <<"change_code_">>, <<"74666">>),
                                    get_code(Phone, 600, 60, <<"change_code_">>),
                                    {ok, Phone};
                                _ ->
                                    {error, ?AFT_ERR_PHONE_EXIST}
                            end;
                        true ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end
            end;
        false ->
            {error, ?AFT_ERR_BAD_PHONE_FORMAT}
    end.


verify_change_phone(User, Server, Phone, Code) ->
    case ejabberd_redis:cmd(["MGET", [<<"change_code_", Phone/binary>>]]) of
        [undefined] ->
            {error, ?AFT_ERR_BAD_CODE};
        [CacheCode] ->
            if CacheCode =:= Code ->
                ejabberd_redis:cmd(["DEL", [<<"varify_token_", User/binary>>]]),
                ejabberd_redis:cmd(["DEL", [<<"change_code_", Phone/binary>>]]),
                case ejabberd_auth_odbc:update_phone(User, Server, Phone) of
                    true -> {ok, Phone};
                    false -> {error, ?ERR_INTERNAL_SERVER_ERROR}
                end;
                true ->
                    {error, ?AFT_ERR_BAD_CODE}
            end
    end.

invite(User, Server, Phone, Type) ->
    case check_phone_number_valid(Phone) of
        true ->
            case ejabberd_auth_odbc:user_info(Server, Phone) of
                {info, _} ->
                    {error, ?AFT_ERR_PHONE_EXIST};
                not_exist ->
                    {ok, [VCardXml]} = mod_vcard_odbc:get_vcard(User, Server),
                    SelfNickname = xml:get_tag_cdata(xml:get_subtag(VCardXml, <<"NICKNAME">>)),
                    SelfPhone = xml:get_tag_cdata(xml:get_subtag(xml:get_subtag(VCardXml, <<"TEL">>), <<"NUMBER">>)),
                    SelfPhone1 = binary:replace(SelfPhone, <<"+86">>, <<>>), %% remove +86.
                    %% this downloadurl should be address of kissnapp on app market after online.
                    DownloadUrl = if
                                      Type =:= <<"0">> -> <<"http://120.24.232.74:8080/setup/kissnapp.apk">>;
                                      Type =:= <<"1">> -> <<"http://120.24.232.74:8080/setup/kissnapp.ipa">>
                                  end,
                    case send_msg(Phone, <<"75554">>, [SelfNickname, SelfPhone1, DownloadUrl]) of
                        ok -> ok;
                        Error -> Error
                    end
            end;
        false ->
            {error, ?AFT_ERR_BAD_PHONE_FORMAT}
    end.

%%%===================================================================
%%% help functions. called by Internal functions.
%%%===================================================================

%% http://www.yuntongxun.com
%%  SMS begin.
%% make sure inets and ssl is started. ejabberd start inet and ssl when started, so this is ok.
%% if test sms function,
%% 1. uncomment this code;
%% 2. change 'AccountID', 'Token', 'AppId', 'URLPrefix' in sendcode/4;
%% 3. add a invoker parameter 'MessageTemplateID' in get_code/5.

-spec now_to_local_string(erlang:timestamp()) -> string().
now_to_local_string({MegaSecs, Secs, MicroSecs}) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} =
        calendar:now_to_local_time({MegaSecs, Secs, MicroSecs}),
    lists:flatten(
        io_lib:format("~4..0w~2..0w~2..0w~2..0w~2..0w~2..0w",
            [Year, Month, Day, Hour, Minute, Second])).

%% Error code: http://docs.yuntongxun.com/
parse_sms_statuscode(Code) ->
    IntCode = list_to_integer(Code),
    if
        IntCode =:= 0 ->
            ok;
        (IntCode =:= 112300) or (IntCode =:= 112306) or (IntCode =:= 112319) or (IntCode =:= 160042) ->
            {error, ?AFT_ERR_BAD_PHONE_FORMAT};
        (IntCode =:= 160015) or (IntCode =:= 160039) or (IntCode =:= 160040) or (IntCode =:= 160041) ->
            {error, ?AFT_ERR_SMS_LIMIT_COUNT};
        (IntCode =:= 112600) or (IntCode =:= 160014) ->
            {error, ?ERR_SERVICE_UNAVAILABLE};
        true ->
            ?ERROR_MSG("[ERROR]:SMS error, Reason=~p~n", [Code]),
            ok
    end.

%% send_code(Phone, MessageTemplateID, Code, SurvivalTime) ->
%%     AccountID = "8a48b551506925be01506e8db71310bd",
%%     Token = "2bfda7eef922411bb6edf4c7bd1f6d83",
%%     AppID = <<"8a48b5515388ec150153980eff6c1446">>,
%%     URLPrefix = "https://app.cloopen.com:8883",
%%
%%     NowString = now_to_local_string(erlang:now()),
%%     SigParameter = jlib:md5_hex(AccountID ++ Token ++ NowString, true),
%%     Authorization = jlib:encode_base64(AccountID ++ ":" ++ NowString),
%%
%%     URL = URLPrefix ++ "/2013-12-26/Accounts/" ++ AccountID ++ "/SMS/TemplateSMS?sig=" ++ SigParameter,
%%     Header = [{"Accept","application/json"}, {"Authorization", Authorization}],
%%     Body =  <<"{\"to\":\"", Phone/binary, "\",\"appId\":\"", AppID/binary,
%%         "\",\"templateId\":\"", MessageTemplateID/binary,"\",\"datas\":[\"", Code/binary,"\",\"", SurvivalTime/binary,"\"]}">>,
%%
%%     Result = httpc:request(post, {URL, Header, "application/json;charset=utf-8", Body}, [], []),
%%     case Result of
%%         {ok, {{_, 200, "OK"}}, _ResponseHeader, ResponseBody} ->
%%             StatusCode = string:substr(ResponseBody, string:str(ResponseBody, "statusCode") + 13, 6),
%%              parse_sms_statuscode(StatusCode);
%%         Error ->
%%             ?ERROR_MSG("[ERROR]:request to sms service failed, reason=~p~n",[Error]),
%%             ok
%%     end.

%% Parameters eg: [<<"66666">>, <<"5">>]
send_msg(Phone, MessageTemplateID, Parameters) ->
    Phone1 = binary:replace(Phone, <<"+86">>, <<>>), %% remove +86.

    AccountID = "8a48b551506925be01506e8db71310bd",
    Token = "2bfda7eef922411bb6edf4c7bd1f6d83",
    AppID = <<"8a48b5515388ec150153980eff6c1446">>,
    URLPrefix = "https://app.cloopen.com:8883",

    NowString = now_to_local_string(erlang:now()),
    SigParameter = jlib:md5_hex(AccountID ++ Token ++ NowString, true),
    Authorization = jlib:encode_base64(AccountID ++ ":" ++ NowString),

    URL = URLPrefix ++ "/2013-12-26/Accounts/" ++ AccountID ++ "/SMS/TemplateSMS?sig=" ++ SigParameter,
    Header = [{"Accept","application/json"}, {"Authorization", Authorization}],

    Data = lists:foldl(fun(E, AccIn) ->
        AccIn1 = if
                     AccIn =:= <<>> -> <<>>;
                     true -> <<AccIn/binary, ",">>
                 end,
        <<AccIn1/binary, "\"", E/binary, "\"">>
    end,
    <<>>,
    Parameters),
    Body =  <<"{\"to\":\"", Phone1/binary, "\",\"appId\":\"", AppID/binary,
    "\",\"templateId\":\"", MessageTemplateID/binary, "\",\"datas\":[", Data/binary, "]}">>,

    Result = httpc:request(post, {URL, Header, "application/json;charset=utf-8", Body}, [], []),
    case Result of
        {ok, {{_, 200, "OK"}}, _ResponseHeader, ResponseBody} ->
            StatusCode = string:substr(ResponseBody, string:str(ResponseBody, "statusCode") + 13, 6),
            parse_sms_statuscode(StatusCode);
        Error ->
            ?ERROR_MSG("[ERROR]:request to sms service failed, reason=~p~n",[Error]),
            ok
    end.


%% SurvicalTime must equal or big then Intelval.
-spec get_code(binary(), integer(), integer(), binary(), binary()) -> {error, _} | {ok, _}.
get_code(Phone, SurvivalTime, Interval, Prefix, MessageTemplateID) ->
    case check_phone_number_valid(Phone) of
        true ->
            Code = list_to_binary(jlib:random_code()),
            Key = <<Prefix/binary, Phone/binary>>,
            TTL = ejabberd_redis:cmd(["TTL", [Key]]),
            Allowed = if
                          (TTL =:= -1) or (TTL =:= -2) -> true;
                          true ->
                              if
                                  SurvivalTime > Interval ->
                                      if TTL > (SurvivalTime - Interval) -> false; true -> true end;
                                  true ->
                                      false
                              end
                      end,

            case Allowed of
                true ->
                    ejabberd_redis:cmd([["DEL", [Key]],
                        ["APPEND", [Key], [Code]],
                        ["EXPIRE", [Key], SurvivalTime]]),

                    %case send_code(Phone, MessageTemplateID, Code, SurvivalTime) of
                    case send_msg(Phone, MessageTemplateID, [Code, SurvivalTime]) of
                        ok -> {ok, Code};
                        Error -> Error
                    end;
                false ->
                    {error, ?AFT_ERR_GET_CODE_SO_QUICKLY}
            end;
        false ->
            {error, ?AFT_ERR_BAD_PHONE_FORMAT}
    end.
%% SMS end.

%% generate a random integer string.
-spec get_code(binary(), integer(), integer(), binary()) ->
                      {error, _} | {ok, _}.
get_code(Phone, SurvivalTime, Interval, Prefix) ->
    case check_phone_number_valid(Phone) of
        true ->
            Code = list_to_binary(jlib:random_code()),
            Key = <<Prefix/binary, Phone/binary>>,
            Allowed = case ejabberd_redis:cmd(["TTL", [Key]]) of
                          -2 ->
                              true;
                          -1 ->
                              true;
                          ST ->
                              if SurvivalTime > Interval ->
                                      Separator = SurvivalTime - Interval,
                                      if ST > Separator -> false;
                                         true -> true
                                      end;
                                 true ->
                                      false
                              end
                      end,

            case Allowed of
                true ->
                    ejabberd_redis:cmd([["DEL", [Key]],
                                        ["APPEND", [Key], [Code]],
                                        ["EXPIRE", [Key], SurvivalTime]]),
                    %% TOFIX: send code message to phone.
                    %%SendMSG = "validation code is" ++ SMSCode ++", the code is valid in 60 seconds,
                    %%          this code is only use for phone app,
                    %%          you shoule make sure this code is knowed only by you"

                    {ok, Code};
                false ->
                    {error, ?AFT_ERR_GET_CODE_SO_QUICKLY}
            end;
        false ->
            {error, ?AFT_ERR_BAD_PHONE_FORMAT}
    end.


get_tag_cdata(false) ->
    <<>>;
get_tag_cdata(Tag) ->
    xml:get_tag_cdata(Tag).

send_registration_notifications(UJID, Source) ->
    Host = UJID#jid.lserver,
    case gen_mod:get_module_opt(Host, ?MODULE, registration_watchers, []) of
        [] -> ok;
        JIDs when is_list(JIDs) ->
            Body = lists:flatten(
                     io_lib:format(
                       "[~s] The account ~s was registered from IP address ~s "
                       "on node ~w using ~p.",
                       [get_time_string(), jlib:jid_to_binary(UJID),
                        ip_to_string(Source), node(), ?MODULE])),
            lists:foreach(
              fun(S) ->
                      case jlib:binary_to_jid(S) of
                          error -> ok;
                          JID ->
                              ejabberd_router:route(
                                jlib:make_jid(<<>>, Host, <<>>),
                                JID,
                                #xmlel{name = <<"message">>,
                                       attrs = [{<<"type">>, <<"chat">>}],
                                       children = [#xmlel{name = <<"body">>,
                                                          children = [#xmlcdata{content = Body}]}]})
                      end
              end, JIDs);
        _ ->
            ok
    end.

check_timeout(undefined) ->
    true;
check_timeout(Source) ->
    Timeout = case ejabberd_config:get_local_option(registration_timeout) of
                  undefined -> 600;
                  TO -> TO
              end,
    if
        is_integer(Timeout) ->
            {MSec, Sec, _USec} = now(),
            Priority = -(MSec * 1000000 + Sec),
            CleanPriority = Priority + Timeout,
            F = fun() ->
                        Treap = case mnesia:read(mod_register_aft_ip, treap,
                                                 write) of
                                    [] ->
                                        treap:empty();
                                    [{mod_register_aft_ip, treap, T}] -> T
                                end,
                        Treap1 = clean_treap(Treap, CleanPriority),
                        case treap:lookup(Source, Treap1) of
                            error ->
                                Treap2 = treap:insert(Source, Priority, [],
                                                      Treap1),
                                mnesia:write({mod_register_aft_ip, treap, Treap2}),
                                true;
                            {ok, _, _} ->
                                mnesia:write({mod_register_aft_ip, treap, Treap1}),
                                false
                        end
                end,

            case mnesia:transaction(F) of
                {atomic, Res} ->
                    Res;
                {aborted, Reason} ->
                    ?ERROR_MSG("mod_register_aft: timeout check error: ~p~n",
                               [Reason]),
                    true
            end;
        true ->
            true
    end.

clean_treap(Treap, CleanPriority) ->
    case treap:is_empty(Treap) of
        true ->
            Treap;
        false ->
            {_Key, Priority, _Value} = treap:get_root(Treap),
            if
                Priority > CleanPriority ->
                    clean_treap(treap:delete_root(Treap), CleanPriority);
                true ->
                    Treap
            end
    end.

remove_timeout(undefined) ->
    true;
remove_timeout(Source) ->
    Timeout = case ejabberd_config:get_local_option(registration_timeout) of
                  undefined -> 600;
                  TO -> TO
              end,
    if
        is_integer(Timeout) ->
            F = fun() ->
                        Treap = case mnesia:read(mod_register_aft_ip, treap,
                                                 write) of
                                    [] ->
                                        treap:empty();
                                    [{mod_register_aft_ip, treap, T}] -> T
                                end,
                        Treap1 = treap:delete(Source, Treap),
                        mnesia:write({mod_register_aft_ip, treap, Treap1}),
                        ok
                end,
            case mnesia:transaction(F) of
                {atomic, ok} ->
                    ok;
                {aborted, Reason} ->
                    ?ERROR_MSG("mod_register_aft: timeout remove error: ~p~n",
                               [Reason]),
                    ok
            end;
        true ->
            ok
    end.

ip_to_string(Source) when is_tuple(Source) -> inet_parse:ntoa(Source);
ip_to_string(undefined) -> "undefined";
ip_to_string(_) -> "unknown".

get_time_string() -> write_time(erlang:localtime()).
%% Function copied from ejabberd_logger_h.erl and customized
write_time({{Y, Mo, D}, {H, Mi, S}}) ->
    io_lib:format("~w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w",
                  [Y, Mo, D, H, Mi, S]).

is_strong_password(Server, Password) ->
    LServer = jlib:nameprep(Server),
    case gen_mod:get_module_opt(LServer, ?MODULE, password_strength, 0) of
        Entropy when is_number(Entropy), Entropy >= 0 ->
            if Entropy == 0 ->
                    true;
               true ->
                    ejabberd_auth:entropy(Password) >= Entropy
            end;
        Wrong ->
            ?WARNING_MSG("Wrong value for password_strength option: ~p",
                         [Wrong]),
            true
    end.


%% valid: not entirely composed of space, enter or empty.
check_nick_name_valid(Nick) ->
    Data = lists:filter(fun(E) ->
        case E of
            <<>> -> false;
            _ -> true end end,
        binary:split(Nick, [<<" ">>, <<"\n">>], [global])),
    if length(Data) >= 1 ->
        true;
        true ->
            false
    end.

check_phone_number_valid(Number) ->
    %% TOFIX: if only use in China, use "^\\+[0-9]{13}$" or "^\\+86[0-9]{11}$" ,  other use "^\\+[0-9]{2, 49}$"
    case re:run(binary_to_list(Number), "^\\+86[0-9]{11}$") of
        nomatch -> false;
        _ -> true
    end.

check_password(_Server, <<>>) ->
    {error, error};
check_password(Server, Password) ->
    case is_strong_password(Server, Password) of
        true ->
            case ejabberd_auth_odbc:prepare_password(Server, Password) of
                false ->
                    {error, invalid_password};
                _ ->
                    {ok, ok}
            end;

        false ->
            {error, weak_password}
    end.

send_welcome_message(JID) ->
    Host = JID#jid.lserver,
    case gen_mod:get_module_opt(Host, ?MODULE, welcome_message, {"", ""}) of
        {"", ""} ->
            ok;
        {Subj, Body} ->
            ejabberd_router:route(
              jlib:make_jid(<<>>, Host, <<>>),
              JID,
              #xmlel{name = <<"message">>, attrs = [{<<"type">>, <<"normal">>}],
                     children = [#xmlel{name = <<"subject">>,
                                        children = [#xmlcdata{content = Subj}]},
                                 #xmlel{name = <<"body">>,
                                        children = [#xmlcdata{content = Body}]}]});
        _ ->
            ok
    end.


generate_token() ->
    jlib:generate_uuid().

generate_jid() ->
    jlib:generate_uuid().