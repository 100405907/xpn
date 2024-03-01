package org.expand;

import org.aspectj.lang.annotation.*;
import org.aspectj.lang.ProceedingJoinPoint;

@Aspect
public class ExpandIntercept {

    @Around("execution(* org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.commitJob(..))")
    public void customCommitJob(ProceedingJoinPoint joinPoint) throws Throwable {
        System.out.println("--------------CUSTOM COMMIT JOB-------------------");        
    }

    @Around("execution(* org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.abortJob(..))")
    public void customAbortJob(ProceedingJoinPoint joinPoint) throws Throwable {
        System.out.println("--------------CUSTOM ABORT JOB-------------------");
    }

}