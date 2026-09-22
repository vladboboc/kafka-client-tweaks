package io.kafkatweaks.spring.errors.recipe;

/** Chapter 18 · "Try again later": the kind of exception a retry is for (a timeout, a 503, a lock conflict). */
public class TransientFailure extends RuntimeException {

    public TransientFailure(String message) {
        super(message);
    }
}
