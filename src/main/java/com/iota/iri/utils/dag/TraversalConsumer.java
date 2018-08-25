package com.iota.iri.utils.dag;

import com.iota.iri.controllers.TransactionViewModel;

public interface TraversalConsumer { void consume(TransactionViewModel currentTransaction) throws Exception; }
