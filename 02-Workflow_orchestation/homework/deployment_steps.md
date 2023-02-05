## Deployment using prefect

After you have a flow, you can deploy it to a Prefect Cloud instance. This is done by creating a `Flow` object and registering it with the Prefect Cloud API. The `Flow` object is created by passing the flow to the `Flow` constructor. The flow can be registered with the Prefect Cloud API by calling the `register` method on the `Flow` object.

Run in your terminal:

```bash
prefect deployment build ./etl_gcs_to_bq.py:etl_parent_flow_gcp -n "GCP Parameterized Flow"
```
This creates a yaml file that can be used to deploy the flow to Prefect Cloud. The yaml file is created in the `flows` directory. The name of the yaml file is the name of the flow with a `.yaml` extension.

If you made changes to the flow, you can update the yaml file by running the following command:

```bash
prefect deployment apply etl_parent_flow_gcp-deployment.yaml
```

prefect agent start  --work-queue "default"
If you want to deploy the flow to Prefect Cloud, you can run the following command:

```bash
prefect agent start  --work-queue "default"
```