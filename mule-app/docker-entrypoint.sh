#!/bin/sh
set -e

exec /opt/mule/bin/mule \
  -M-Dsf.system.api.port="${SF_SYSTEM_API_PORT}" \
  -M-Dfeasibility.system.api.port="${FEASIBILITY_SYSTEM_API_PORT}" \
  -M-Dprocess.api.port="${PROCESS_API_PORT}" \
  -M-Dexperience.api.port="${EXPERIENCE_API_PORT}" \
  -M-Dfeasibility.host="${FEASIBILITY_HOST}" \
  -M-Dfeasibility.port="${FEASIBILITY_PORT}" \
  -M-Dsf.system.api.host="${SF_SYSTEM_API_HOST}" \
  -M-Dfeasibility.system.api.host="${FEASIBILITY_SYSTEM_API_HOST}" \
  -M-Dprocess.api.host="${PROCESS_API_HOST}" \
  -M-Dsfdc.consumer.key="${SFDC_CONSUMER_KEY}" \
  -M-Dsfdc.username="${SFDC_USERNAME}" \
  -M-Dsfdc.instance.host="${SFDC_INSTANCE_HOST}" \
  -M-Dsfdc.jwt.audience="${SFDC_JWT_AUDIENCE}" \
  -M-Dsfdc.jwt.keystore.path="${SFDC_JWT_KEYSTORE_PATH}" \
  -M-Dsfdc.jwt.keystore.password="${SFDC_JWT_KEYSTORE_PASSWORD}" \
  -M-Dsfdc.jwt.key.alias="${SFDC_JWT_KEY_ALIAS}" \
  -M-Dactivemq.broker.url="${ACTIVEMQ_BROKER_URL}"
