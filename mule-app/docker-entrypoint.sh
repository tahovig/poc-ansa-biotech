#!/bin/sh
set -e

# App properties are baked directly into wrapper.conf as fixed
# wrapper.java.additional.N entries rather than passed as -M-D command-line
# flags. Found while debugging Task 12's Docker deploy: passing more than
# 8 -M-D flags on the bin/mule command line silently drops everything past
# the 8th (confirmed via /proc/<pid>/cmdline on the launched JVM, not just
# `ps aux`'s truncated display) -- the same fragile wrapper.conf/-M-D
# argument-merging machinery already implicated in the JPMS module-path
# bug fixed at build time in the Dockerfile. The distribution's own
# entries plus the Dockerfile's module-path fix fill indices 1-24; these
# continue contiguously from 25 -- whatever parses wrapper.java.additional.N
# stops scanning at the first missing index rather than treating them as a
# sparse set (confirmed by an earlier attempt starting at 30, leaving a
# 25-29 gap, which silently dropped all of these).
cat >> /opt/mule/conf/wrapper.conf <<EOF
wrapper.java.additional.25=-Dsf.system.api.port=${SF_SYSTEM_API_PORT}
wrapper.java.additional.26=-Dfeasibility.system.api.port=${FEASIBILITY_SYSTEM_API_PORT}
wrapper.java.additional.27=-Dprocess.api.port=${PROCESS_API_PORT}
wrapper.java.additional.28=-Dexperience.api.port=${EXPERIENCE_API_PORT}
wrapper.java.additional.29=-Dfeasibility.host=${FEASIBILITY_HOST}
wrapper.java.additional.30=-Dfeasibility.port=${FEASIBILITY_PORT}
wrapper.java.additional.31=-Dsf.system.api.host=${SF_SYSTEM_API_HOST}
wrapper.java.additional.32=-Dfeasibility.system.api.host=${FEASIBILITY_SYSTEM_API_HOST}
wrapper.java.additional.33=-Dprocess.api.host=${PROCESS_API_HOST}
wrapper.java.additional.34=-Dsfdc.instance.host=${SFDC_INSTANCE_HOST}
wrapper.java.additional.35=-Dsfdc.auth.host=${SFDC_AUTH_HOST}
wrapper.java.additional.36=-Dsfdc.auth.port=${SFDC_AUTH_PORT}
wrapper.java.additional.37=-Dactivemq.broker.url=${ACTIVEMQ_BROKER_URL}
wrapper.java.additional.38=-Dactivemq.username=${ACTIVEMQ_USERNAME}
wrapper.java.additional.39=-Dactivemq.password=${ACTIVEMQ_PASSWORD}
EOF

exec /opt/mule/bin/mule console
