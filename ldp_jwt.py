# JWT
SECRET_KEY=change_me_to_a_long_random_string
JWT_ALGORITHM=HS256
TOKEN_TTL_HOURS=12
ALLOWED_DOMAIN_SUFFIX=@adroitcorp.adroit.com

# LDAP/AD
LDAP_URI=ldaps://ldap.adroitcorp.adroit.com:636
LDAP_USE_SSL=true
# If you bind directly as the service account UPN (user@domain), you don't need a bind DN.
# If your directory requires searching first, set these to a service account that can search.
LDAP_BIND_DN=  # e.g. CN=svc-ldap,OU=Service Accounts,DC=adroitcorp,DC=adroit,DC=com
LDAP_BIND_PASSWORD=
LDAP_BASE_DN=DC=adroitcorp,DC=adroit,DC=com

# Ozone S3
OZONE_S3_ENDPOINT=http://ozone-s3-gateway:9878
OZONE_S3_ACCESS_KEY=YOUR_OZONE_S3_ACCESS_KEY
OZONE_S3_SECRET_KEY=YOUR_OZONE_S3_SECRET_KEY
OZONE_BUCKET=documents