# Description
This helper script essentially transaltes ETS2 Linux telemtry to windows for apps running with windows telemtry and hosts it in a websocket for e.g. trucknav - you need to install the scs telemtry.so

# Installation
npm install ws

or ``npm install``

# Contributing
currently the code is quite buggy as I let AI throw it together at like 3:00 in the morning and therfore expect bugs. I am ready to accept bug fixes just create a PR and a info what you fixed or added

# References
- You need to run Linux (however proton is specifically not required)
- You need to have [SCS-SDK-Plugin](https://github.com/truckermudgeon/scs-sdk-plugin) (the Linux one, MacOS maybe works, Windows will not work there is already a native one so you do not need to use the script)
- You can use e.g. this plugin with my tool (which was the main reason why I let AI code it in the first place) [TruckNav](https://github.com/Rares-Muntean/TruckNav-Sim)
