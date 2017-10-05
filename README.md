[![Build status](https://ci.appveyor.com/api/projects/status/bava7aw8qci7tcve/branch/master?svg=true)](https://ci.appveyor.com/project/vostok/airlock-consumer/branch/master)

## RoutingKey format

Top-level separator is `.`:
`{part1}.{part2}.{part3}`

`[a-zA-Z0-9]` and `-` are the only symbols allowed in parts. `.` inside parts must be replaced with `-`.

### Requirements on typical routing keys

Message type            | Routing key format
------------------------|-------------------
Logs                    | {Project}.{Environment}.{ServiceName}.logs
Traces                  | {Project}.{Environment}.{ServiceName}.traces
Final metric            | {Project}.{Environment}.{ServiceName}.metrics
Metric events           | {Project}.{Environment}.{ServiceName}.app-events
Metric events by traces | {Project}.{Environment}.{ServiceName}.trace-events
