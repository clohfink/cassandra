# Built by newt registry build from the Jib image, which is built during gradle publish
FROM cde-nfcassandra:latest

RUN echo '. /etc/cassandra-aliases.sh' >> /etc/bash.bashrc

HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
  CMD bash -c 'echo > /dev/tcp/$(hostname -i)/7104' || exit 1
