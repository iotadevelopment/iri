package com.iota.iri.service.transactionpruning;

/**
 * Functional interface for the lambda function that takes care of parsing a specific job from its serialized String
 * representation into the corresponding object in memory.
 *
 * @see TransactionPruner#registerParser(Class, JobParser) to register the parser
 */
@FunctionalInterface
public interface JobParser {
    TransactionPrunerJob parse(String input) throws TransactionPruningException;
}
