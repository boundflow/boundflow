alias bf="boundflow --server http://localhost:50051 --api-key $BOUNDFLOW_API_KEY"

1. Show worker_v1.py code

2. bf tenant create acme-demo
   bf workflow create refund $TENANT_ID --version 1

3. bf policy runtime $WF_ID analyst --file runtime_policy.json
   bf policy lifecycle set-agent $WF_ID analyst --file agent_lifecycle_policy.json
   bf policy lifecycle set-workflow $WF_ID --file workflow_lifecycle_policy.json

4. bf workflow activate $WF_ID
   bf workflow invoke $WF_ID
   bf workflow approve $WF_ID $APPROVAL_ID
   bf workflow runs $WF_ID
   bf workflow get $WF_ID

5. Show worker_v2.py code (new prompt, version 2)

6. bf workflow set-config $WF_ID --version 2 --repeat 30

7. [wait for pending_approval]
   bf workflow reject $WF_ID $APPROVAL_ID

8. bf workflow get $WF_ID          (version back to 1, already)
   [wait for next periodic run]
   bf audit workflow $WF_ID
