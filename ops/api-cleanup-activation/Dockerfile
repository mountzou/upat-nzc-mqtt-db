# Cleanup overlay: dependencies and Python are inherited unchanged.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG SOURCE_REVISION
ARG RECIPE_REVISION
LABEL org.opencontainers.image.revision=${SOURCE_REVISION} \
      com.schoolheroz.api-cleanup.recipe-revision=${RECIPE_REVISION}
RUN rm -rf /app/monitoring /app/readers /app/__pycache__
COPY payload/ /app/
COPY source-manifest.json /usr/local/share/schoolheroz-release/source-manifest.json
