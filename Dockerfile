FROM ghcr.io/gleam-lang/gleam:v1.14.0-erlang-alpine AS build
COPY . /app
RUN cd /app && gleam export erlang-shipment

FROM erlang:28-alpine
RUN addgroup --system app && adduser --system app -G app
COPY --from=build /app/build/erlang-shipment /app
USER app
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["run"]
