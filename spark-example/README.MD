### SparkSubmit 执行流程
    通过如下命令 提交Spark任务 到Yarn集群
    bin/sparkSubmit \
    --master yarn  \
    --deploy-mode cluster \
    --class org.apache.spark.examples.SparkPi \
    --executor-memory 1G \
    --total-executor-cores 2 \
    
####源码执行流程如下
   
**1.SparkSubmit**
    
    	--main()
        //封装提交参数 (通过命令提交的参数)
        -- val appArgs = new SparkSubmitArguments(args);
            // action 返回SUBMIT
            action = Option(action).getOrElse(SUBMIT)
            
        // 通过模式匹配 匹配到 action= SUBMIT. 所以,执行submit方法
        -- submit(appArgs)
        
            // 主备提交环境
            -- val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)
                //Yarn-Cluster 
                childMainClass = "org.apache.spark.deploy.yarn.Client"
                //Yarn-Client 
                childMainClass = args.mainClass	//如果是Client模式 childMainClass 则为 通过名传入的 class
                
            
            -- doRunMain()
                
                -- runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
                
                    //获取 MainClass 当前是集群模式 所以 mainClass = "org.apache.spark.deploy.yarn.Client" 
                    -- mainClass = Utils.classForName(childMainClass)
                    
                    //获取 MainClass的 main()方法
                    -- mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
                    
                    //执行 org.apache.spark.deploy.yarn.Client.main() 
                    -- mainMethod.invoke(null, childArgs.toArray)
                    
**2.org.apache.spark.deploy.yarn.Client**	
    

    //需要导入
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-yarn_2.11</artifactId>
        <version>2.1.1</version>
    </dependency>
    
    -- main()
    // 封装参数
    -- val args = new ClientArguments(argStrings)
    
    // private val yarnClient = YarnClient.createYarnClient
    -- new Client(args, sparkConf)	//返回一个 YarnClient
    
    -- yarnClient.run()
        
        // 提交应用,返回appId
        -- this.appId = submitApplication()
            
            //1.连接到后端
            //2.设置证书
            //3.初始化yarnClient
            //4.启动yarnClient
            
            // 设置上下文 启动AM
             
            --  val containerContext = createContainerLaunchContext(newAppResponse)
                //封装 指令. 赋值到 amContainer 
                --val commands = prefixEnv ++ Seq(
                    YarnSparkHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java", "-server"
                  ) ++
                  javaOpts ++ amArgs ++
                  Seq(
                    "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
                    "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
                
            // 获取上下文环境
            --  val appContext = createApplicationSubmissionContext(newApp, containerContext)
            
            //提交任务到Yarn集群
            -- yarnClient.submitApplication(appContext) 
    				
    	
    					
    		