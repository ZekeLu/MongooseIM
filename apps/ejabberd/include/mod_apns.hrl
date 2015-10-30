
%% Connection Parameters

-define(EPOCH, 62167219200).
-define(MAX_PAYLOAD, 256).

-record(apns_connection, {
    apple_host = "gateway.sandbox.push.apple.com" :: string(),
    apple_port = 2195 :: integer(),
    cert = undefined :: undefined | binary(),
    cert_file = "priv/cert.pem" :: string(),
    key = undefined :: undefined | {'RSAPrivateKey'| 'DSAPrivateKey' | 'ECPrivateKey' | 'PrivateKeyInfo', binary()},
    key_file = undefined :: undefined | string(),
    cert_password = undefined :: undefined | string(),
    timeout = 30000 :: integer(),
    error_fun :: fun((binary(), mod_apns:status()) -> stop | _),
    feedback_host = "feedback.sandbox.push.apple.com" :: string(),
    feedback_port = 2196 :: integer(),
    feedback_fun :: fun(({calendar:datetime(), string()}) -> _),
    feedback_timeout = 30 * 60 * 1000 :: pos_integer()
}).

-record(apns_msg, {
    id = mod_apns:message_id() :: binary(),
    expiry = mod_apns:expiry(86400) :: non_neg_integer(), %% default = 1 day
    device_token :: string(),
    content_available = false :: boolean(),
    alert = none :: none | mod_apns:alert(),
    badge = none :: none | integer(),
    sound = none :: none | mod_apns:apns_str(),
    apns_extra = [] :: none | [{atom(), integer()|boolean()|string()}],
    extra = [] :: proplists:proplist(),
    priority = 10 :: integer()
}).


-record(loc_alert, {
    body = none :: none | mod_apns:apns_str(),
    action = none :: none | mod_apns:apns_str(),
    key = "" :: mod_apns:apns_str(),
    args = [] :: [mod_apns:apns_str()],
    image = none :: none | mod_apns:apns_str()
}).