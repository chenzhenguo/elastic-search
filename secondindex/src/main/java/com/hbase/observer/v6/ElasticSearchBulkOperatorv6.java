package com.hbase.observer.v6;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ElasticSearchBulkOperatorv6{
    private static final Log LOG = LogFactory.getLog(ElasticSearchBulkOperatorv6.class);

    private static final int MAX_BULK_COUNT = 500;

    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static final Lock commitLock = new ReentrantLock();

    private static ScheduledExecutorService scheduledExecutorService = null;

    static {
        // init es bulkRequestBuilder
        bulkRequestBuilder = ESClientv6.client.prepareBulk();
        //--------------------------------------  目前此refresh接口不存在了
//        bulkRequestBuilder.setRefresh(true);

        // init thread pool and set size 1
        scheduledExecutorService = Executors.newScheduledThreadPool(1);

        // create beeper thread( it will be sync data to ES cluster)
        // use a commitLock to protected bulk es as thread-save
        final Runnable beeper = new Runnable() {
            public void run() {
                commitLock.lock();
                try {
                    bulkRequest(0);
                } catch (Exception ex) {
//                    System.out.println(ex.getMessage());
                    LOG.error("Time Bulk "+ ex.getMessage());
                } finally {
                    commitLock.unlock();
                }
            }
        };

        // set time bulk task
        // set beeper thread(10 second to delay first execution , 30 second period between successive executions)
        scheduledExecutorService.scheduleAtFixedRate(beeper, 10, 30, TimeUnit.SECONDS);

    }

    /**
     * shutdown time task immediately
     */
    public static void shutdownScheduEx() {
        if (null != scheduledExecutorService && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    /**
     * bulk request when number of builders is grate then threshold
     *
     * @param threshold
     */
    private static void bulkRequest(int threshold) {
		if (bulkRequestBuilder.numberOfActions() > threshold) {
			try {
				BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
				if (!bulkItemResponse.hasFailures()) {
					LOG.error(bulkItemResponse.buildFailureMessage());
				}
				bulkRequestBuilder = ESClientv6.client.prepareBulk();
			} catch (Exception e) {
				 LOG.error(" Bulk Request " + " index error : " + e.getMessage());  
	                LOG.error("Reconnect the ES server...");  
	                List<DocWriteRequest> tempRequests = bulkRequestBuilder.request().requests();  
	                ESClientv6.client.close();  
	                try {
						ESClientv6.initEsClient();
					} catch (Exception e1) {
						LOG.error("重连es client 失败"+e1.getMessage());  
					};  
	                bulkRequestBuilder = ESClientv6.client.prepareBulk();  
	                bulkRequestBuilder.request().add(tempRequests);  
			}
		}
    }

    /**
     * add update builder to bulk
     * use commitLock to protected bulk as thread-save
     * @param builder
     */
    public static void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
        	LOG.error("============004");
            bulkRequestBuilder.add(builder);
            LOG.error("============005");
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" update Bulk "  + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * add delete builder to bulk
     * use commitLock to protected bulk as thread-save
     *
     * @param builder
     */
    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" delete Bulk "  + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }
}
