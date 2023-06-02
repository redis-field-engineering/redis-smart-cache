FROM grafana/grafana:9.5.2
ENV GF_INSTALL_PLUGINS=redis-datasource

COPY defaults.ini /etc/grafana/grafana.ini
COPY grafana.db /var/lib/grafana/grafana.db

USER root
RUN chown grafana /etc/grafana/grafana.ini
RUN chown grafana /var/lib/grafana/grafana.db
