package local.dev.replication;

import java.io.Serializable;

public class Message implements Serializable {
  private static final long serialVersionUID = 1L;

  enum Action {
    READ_LAST,
    READ_ALL,
  }

  private Action action;
  private String replyTo;
  private String content;

  public Message(Action action, String replyTo, String content) {
    this.action = action;
    this.replyTo = replyTo;
    this.content = content;
  }

  public Action getAction() {
    return action;
  }

  public String getReplyTo() {
    return replyTo;
  }

  public String getContent() {
    return content;
  }
}
