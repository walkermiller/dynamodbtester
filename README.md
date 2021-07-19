## Requirements and setup
You will need to have a DynamoDB table named Test that has a primary key of ResourceId.

You will need a working Kubernetes cluster with a service account named dynamodbtester that has IAM permissions to access the DynamoDB Table. You can use eksctl to define this service account (note this command gives the service account full access to all DynamoDB resources. You will want to create a policy that is more restrictive to use.):

```
eksctl create iamserviceaccount --cluster=lab --name=dynamodbtester --namespace=default --attach-policy-arn=arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess --approve
```
You will also need to have helm installed locally

## Running
Clone the Repo
```
git clone https://github.com/walkermiller/dynamodbtester.git
```
Modify the values.yaml file to match your desired values
```
cd dydbtester
vi values.yaml
```
Install the chart:
```
helm install -f values.yaml dynamodbtester . 
```
Watch the progress:
```
kubectl get pods --watch
```
You can look at the logs of the jobs as they complete:
```
❯ kubectl logs dydbtester-get-99-wmktm        
Processing 100000 Messgaes [100000/100000] ███████████████████████████ 100% | 9s
```

you can see the output of all job logs like this:
```
❯ kubectl logs -laction=get
```

When you are finished, remove all the jobs:
```
helm uninstall dynamodbtester
```

