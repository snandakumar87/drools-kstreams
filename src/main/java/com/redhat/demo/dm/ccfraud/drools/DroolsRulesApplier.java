package com.redhat.demo.dm.ccfraud.drools;

import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.CreditCardTransactionRepository;
import com.redhat.demo.dm.ccfraud.InMemoryCreditCardTransactionRepository;
import com.redhat.demo.dm.ccfraud.domain.CreditCardTransaction;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionClock;
import org.kie.api.time.SessionPseudoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class DroolsRulesApplier {
    private static KieSession KIE_SESSION;
    private static CreditCardTransactionRepository cctRepository = new InMemoryCreditCardTransactionRepository();
    private static final Logger LOGGER = LoggerFactory.getLogger(DroolsRulesApplier.class);

    public DroolsRulesApplier(String sessionName) {
        KIE_SESSION = DroolsSessionFactory.createDroolsSession(sessionName);
    }

    /**
     * Applies the loaded Drools rules to a given String.
     *
     * @param value the String to which the rules should be applied
     * @return the String after the rule has been applied
     */
    public void getSession(CreditCardTransaction value) {
        KIE_SESSION.insert(value);
        KIE_SESSION.fireAllRules();

    }

    public String processTransaction(String ccTransaction) {



        CreditCardTransaction creditCardTransaction = new Gson().fromJson(ccTransaction,CreditCardTransaction.class);

        getSession(creditCardTransaction);

        // Retrieve all transactions for this account
        Collection<CreditCardTransaction> ccTransactions = cctRepository
                .getCreditCardTransactionsForCC(creditCardTransaction.getCreditCardNumber());

        LOGGER.debug("Found '" + ccTransactions.size() + "' transactions for creditcard: '" + creditCardTransaction.getCreditCardNumber() + "'.");


        // Insert transaction history/context.
        LOGGER.debug("Inserting credit-card transaction context into session.");
        for (CreditCardTransaction nextTransaction : ccTransactions) {
            insert(KIE_SESSION, "Transactions", nextTransaction);
        }
        // Insert the new transaction event
        LOGGER.debug("Inserting credit-card transaction event into session.");
        insert(KIE_SESSION, "Transactions", creditCardTransaction);
        // And fire the rules.
        KIE_SESSION.fireAllRules();

        // Dispose the session to free up the resources.
        KIE_SESSION.dispose();
        return ccTransaction;

    }

    /**
     * CEP insert method that inserts the event into the Drools CEP session and programatically advances the session clock to the time of
     * the current event.
     *
     * @param kieSession
     *            the session in which to insert the event.
     * @param stream
     *            the name of the Drools entry-point in which to insert the event.
     * @param cct
     *            the event to insert.
     *
     * @return the {@link FactHandle} of the inserted fact.
     */
    private  FactHandle insert(KieSession kieSession, String stream, CreditCardTransaction cct) {
        SessionClock clock = kieSession.getSessionClock();
        if (!(clock instanceof SessionPseudoClock)) {
            String errorMessage = "This fact inserter can only be used with KieSessions that use a SessionPseudoClock";
            LOGGER.error(errorMessage);
            throw new IllegalStateException(errorMessage);
        }
        SessionPseudoClock pseudoClock = (SessionPseudoClock) clock;
        EntryPoint ep = kieSession.getEntryPoint(stream);

        // First insert the event
        FactHandle factHandle = ep.insert(cct);
        // And then advance the clock.

        long advanceTime = cct.getTimestamp() - pseudoClock.getCurrentTime();
        if (advanceTime > 0) {
            LOGGER.debug("Advancing the PseudoClock with " + advanceTime + " milliseconds.");
            pseudoClock.advanceTime(advanceTime, TimeUnit.MILLISECONDS);
        } else {
            // Print a warning when we don't need to advance the clock. This usually means that the events are entering the system in the
            // incorrect order.
            LOGGER.warn("Not advancing time. CreditCardTransaction timestamp is '" + cct.getTimestamp() + "', PseudoClock timestamp is '"
                    + pseudoClock.getCurrentTime() + "'.");
        }
        return factHandle;
    }
}
