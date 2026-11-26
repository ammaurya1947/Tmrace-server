# Typing Maestro Race - Render server

## Deploy (5 steps)
1. Create a new GitHub repo and upload ALL files from this zip (Dockerfile, tmrace.cpp, render.yaml).
2. Render dashboard -> New + -> Web Service -> connect that repo. Runtime = Docker (render.yaml sets it). Plan = Free is fine.
3. Health Check Path = /health (already in render.yaml). Wait for "Live".
4. Your link is  https://YOUR-NAME.onrender.com  -> in the game use:  wss://YOUR-NAME.onrender.com
   (Settings -> Server address, or run `tmrace --client --host wss://YOUR-NAME.onrender.com`)
5. Open the https link in a browser: it must show "Typing Maestro Race server is running."

## Notes
* Render web services only accept HTTP/WebSocket, not raw TCP - so the game connects with wss:// (WebSocket over TLS).
  The same server also speaks raw TCP (host:port) for VPS/LAN use. One binary, one port ($PORT is read automatically).
* Free plan sleeps after ~15 min idle. First player waits ~30-60 s while it wakes (the game shows "waking up"). Paid plan = always on.
* Rooms live in memory: a restart/redeploy clears rooms (players can reconnect within 15 s during a race).
* wss:// in the game needs OpenSSL (libssl) on the player's device; it is loaded at runtime. Windows/Linux/macOS usually have it
  (Windows: put libssl-3-x64.dll + libcrypto-3-x64.dll next to the exe). Android: bundle libssl.so/libcrypto.so in your APK.
* TLS encrypts traffic but the client does not verify the server certificate (see main README).
