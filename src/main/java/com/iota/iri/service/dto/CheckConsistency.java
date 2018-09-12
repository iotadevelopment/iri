package com.iota.iri.service.dto;

public class CheckConsistency extends AbstractResponse {

    private boolean state;
    
    private String info;

    public static AbstractResponse create(boolean state, String info) {
        CheckConsistency res = new CheckConsistency();
        res.state = state;
        res.info = info;
        return res;
    }

    /**
     * The balances of the transaction
     *
     * @return The balances.
     */
    public boolean getState() {
        return state;
    }
    
    /**
     * Information about the balances
     *
     * @return The info.
     */
    public String getInfo() {
        return info;
    }
}
