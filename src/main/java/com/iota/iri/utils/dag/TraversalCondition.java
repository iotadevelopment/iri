package com.iota.iri.utils.dag;

import com.iota.iri.controllers.TransactionViewModel;

public interface TraversalCondition { boolean check(TransactionViewModel currentTransaction) throws Exception; }
