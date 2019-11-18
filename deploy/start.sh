kubectl apply -f crd.yaml
cp -r ../bin/ ./
nohup bin/scheduler --v=5 --config scheduler/config/batch_scheduler_config.json 2>&1 &