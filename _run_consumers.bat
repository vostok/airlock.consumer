cd Vostok.AirlockConsumer.Logs
start "Logs" dotnet run
cd ..

ping -n 5 localhost

cd Vostok.AirlockConsumer.Tracing
start "Tracing" dotnet run
cd ..
