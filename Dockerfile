# Typing Maestro Race - dedicated server for Render (Docker web service)
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY tmrace.cpp .
RUN g++ -std=c++17 -O2 -DTM_SERVER_ONLY tmrace.cpp -o tmserver -pthread -static-libstdc++ -static-libgcc

FROM debian:bookworm-slim
COPY --from=build /src/tmserver /usr/local/bin/tmserver
ENV PORT=10000
EXPOSE 10000
CMD ["tmserver", "--server"]
