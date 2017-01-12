package edu.caltech.nanodb.expressions;


/**
 * This exception is thrown when there is an error during the evaluation of an
 * expression.
 **/
public class ExpressionException extends RuntimeException {

  /** Construct an expression exception with no message. **/
  public ExpressionException() {
    super();
  }


  /** Construct an expression exception with the specified message. **/
  public ExpressionException(String msg) {
    super(msg);
  }


  /**
   * Construct an expression exception with the specified cause and no message.
   **/
  public ExpressionException(Throwable cause) {
    super(cause);
  }


  /**
   * Construct an expression exception with the specified message and cause.
   **/
  public ExpressionException(String msg, Throwable cause) {
    super(msg, cause);
  }
}

