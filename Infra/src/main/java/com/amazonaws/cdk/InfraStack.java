package com.amazonaws.cdk;

import io.github.cdklabs.cdknag.NagPackSuppression;
import io.github.cdklabs.cdknag.NagSuppressions;
import software.amazon.awscdk.*;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.kms.Key;
import software.amazon.awscdk.services.lambda.Architecture;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.notifications.LambdaDestination;
import software.amazon.awscdk.services.scheduler.alpha.CronOptionsWithTimezone;
import software.amazon.awscdk.services.scheduler.alpha.Schedule;
import software.amazon.awscdk.services.scheduler.alpha.ScheduleExpression;
import software.amazon.awscdk.services.scheduler.alpha.ScheduleTargetInput;
import software.amazon.awscdk.services.scheduler.targets.alpha.LambdaInvoke;
import software.constructs.Construct;

import java.util.List;
import java.util.Map;
import java.util.Objects;


public class InfraStack extends Stack {
    public InfraStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public InfraStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // Access the Context file to get values for Parameters
        String instanceId = this.getNode().tryGetContext("InstanceId").toString();
        String agentTransferFlowId = this.getNode().tryGetContext("DefaultAgentTransferFlowId").toString();
        String processQuickConnectType = this.getNode().tryGetContext("ProcessQuickConnectType").toString();

        Key loggingBucketKey = Key.Builder.create(this, "LoggingBucketKey")
                .enableKeyRotation(true)
                .pendingWindow(Duration.days(7))
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();


        Bucket loggingBucket = Bucket.Builder.create(this, "LoggingBucket")
                .enforceSsl(true)
                .encryption(BucketEncryption.KMS)
                .encryptionKey(loggingBucketKey)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .versioned(true)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                .build();

        Key sourceBucketKey = Key.Builder.create(this, "SourceBucketKey")
                .enableKeyRotation(true)
                .pendingWindow(Duration.days(7))
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();

        Bucket sourceBucket = Bucket.Builder.create(this, "SourceBucket")
                .enforceSsl(true)
                .versioned(true)
                .encryption(BucketEncryption.KMS)
                .encryptionKey(sourceBucketKey)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .serverAccessLogsBucket(loggingBucket)
                .serverAccessLogsPrefix("sourceBucket/")
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                .build();

        // Create an IAM role for the Lambda function
        Role lambdaRole = Role.Builder.create(this, "LambdaRole")
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .build();

        // Create a policy statement for CloudWatch Log Group
        PolicyStatement logGroupStatement = PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("logs:CreateLogGroup"))
                .resources(List.of("arn:" + getPartition() + ":logs:" + getRegion() + ":" + getAccount() + ":*"))
                .build();

        // Create a policy statement for CloudWatch Log Group
        PolicyStatement connectStatement = PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("connect:SearchUsers",
                        "connect:SearchQuickConnects",
                        "connect:DescribeUser",
                        "connect:ListQueues",
                        "connect:CreateQuickConnect",
                        "connect:DescribeQuickConnect",
                        "connect:UpdateQuickConnectName",
                        "connect:ListQuickConnects",
                        "connect:UpdateQuickConnectConfig",
                        "connect:AssociateQueueQuickConnects",
                        "connect:DeleteQuickConnect"))
                .resources(List.of("arn:" + getPartition() + ":connect:" + getRegion() + ":" + getAccount() + ":instance/" + instanceId,
                        "arn:" + getPartition() + ":connect:" + getRegion() + ":" + getAccount() + ":instance/" + instanceId + "/agent/*",
                        "arn:" + getPartition() + ":connect:" + getRegion() + ":" + getAccount() + ":instance/" + instanceId + "/queue/*",
                        "arn:" + getPartition() + ":connect:" + getRegion() + ":" + getAccount() + ":instance/" + instanceId + "/transfer-destination/*",
                        "arn:" + getPartition() + ":connect:" + getRegion() + ":" + getAccount() + ":instance/" + instanceId + "/contact-flow/*"))
                .build();

        lambdaRole.addToPolicy(connectStatement);
        lambdaRole.addToPolicy(logGroupStatement);

        Function quickConnectFunction = Function.Builder.create(this, "QuickConnectFunction")
                .runtime(Runtime.JAVA_21)
                .architecture(Architecture.X86_64)
                .handler("com.amazonaws.lambda.QuickConnectFunction")
                .memorySize(1024)
                .timeout(Duration.minutes(5))
                .code(Code.fromAsset("../assets/QuickConnectFunction.jar"))
                .environment(Map.of(
                        "Source_S3_Bucket", sourceBucket.getBucketName(),
                        "Instance_Id", instanceId,
                        "Default_Agent_Transfer_Flow_Id", agentTransferFlowId,
                        "Process_Quick_Connect_Type", processQuickConnectType
                ))
                .role(lambdaRole)
                .build();

        // Add Object Created Notification to Source Bucket
        LambdaDestination lambdaDestination = new LambdaDestination(quickConnectFunction);
        sourceBucket.addObjectCreatedNotification(lambdaDestination);

        // AWS Lambda Execution Permission
        sourceBucket.grantRead(quickConnectFunction);

        // Create a policy statement for CloudWatch Logs
        PolicyStatement logsStatement = PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("logs:CreateLogStream", "logs:PutLogEvents"))
                .resources(List.of("arn:" + getPartition() + ":logs:" + getRegion() + ":" + getAccount() + ":log-group:/aws/lambda/" + quickConnectFunction.getFunctionName() + ":*"))
                .build();


        Objects.requireNonNull(quickConnectFunction.getRole()).attachInlinePolicy(Policy.Builder.create(this, "LogsPolicy")
                .document(PolicyDocument.Builder.create()
                        .statements(List.of(logsStatement))
                        .build())
                .build());

        // Create the Lambda target
        LambdaInvoke target = LambdaInvoke.Builder.create(quickConnectFunction)
                .input(ScheduleTargetInput.fromText("""
                        { "Frequency": "Weekly" }
                        """))
                .retryAttempts(0)
                .build();

        // Create the schedule using a cron expression for daily execution at 12:00 UTC
        Schedule schedule = Schedule.Builder.create(this, "UserTypeSchedule")
                .schedule(ScheduleExpression.cron(CronOptionsWithTimezone.builder()
                        .hour("8")
                        .minute("0")
                        .weekDay("MON")
                        .timeZone(TimeZone.AMERICA_LOS_ANGELES)
                        .build()))
                .target(target)
                .description("Weekly QuickConnect Lambda Invoke at 8am PST")
                .build();

        //CDK NAG Suppression's
        NagSuppressions.addResourceSuppressionsByPath(this, "/InfraStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource",
                List.of(NagPackSuppression.builder()
                                .id("AwsSolutions-IAM4")
                                .reason("Internal CDK lambda needed to apply bucket notification configurations")
                                .appliesTo(List.of("Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"))
                                .build(),
                        NagPackSuppression.builder()
                                .id("AwsSolutions-IAM5")
                                .reason("Internal CDK lambda needed to apply bucket notification configurations")
                                .appliesTo(List.of("Resource::*"))
                                .build()));

        NagSuppressions.addStackSuppressions(this, List.of(NagPackSuppression.builder()
                .id("AwsSolutions-IAM5")
                .reason("""
                        Lambda needs access to create Log group and put log events which require *.\s
                        Resources have limited access to the Amazon Connect Instance and other sub-resources of those connect instance have to be '*'.\s
                        Lambda have given read-only to S3 Source Bucket.\s
                        EventBridge Scheduler role is given access to invoke Lambda.""")
                .build()));
    }
}
