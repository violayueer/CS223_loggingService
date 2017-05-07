package com.yyy.tippers.logging.utils;

/**
 * Created by yiranwang on 5/3/17.
 */

import org.springframework.data.gemfire.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Repository
public interface TransactionRepository extends CrudRepository<TransactionLog, AtomicInteger>{

    TransactionLog findByTxid(AtomicInteger txid);

    List<TransactionLog> findAll();

    @Query("select MAX(txid) from /transactionLog")
    int findLargestTxid();
}
