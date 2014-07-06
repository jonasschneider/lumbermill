# Lumbermill

This is a Go app which takes Heroku Log drains and parses the router and dyno information, and then pushes metrics to Riemann.

## Setup

### Install on Heroku
```
heroku create -b https://github.com/kr/heroku-buildpack-go.git <lumbermill_app>
heroku config:set RIEMANN_ADDRESS="<host:port>"

git push heroku master

heroku drains:add https://<lumbermill_app>.herokuapp.com/drain --app <the-app-to-mill-for>
```

You'll then start getting metrics in your influxdb host!
