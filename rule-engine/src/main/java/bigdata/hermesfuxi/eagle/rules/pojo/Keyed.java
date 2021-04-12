package bigdata.hermesfuxi.eagle.rules.pojo;

public class Keyed<IN, KEY, ID> {
  private IN wrapped;
  private KEY key;
  private ID id;

  public Keyed() {
  }

  public Keyed(IN wrapped, KEY key, ID id) {
    this.wrapped = wrapped;
    this.key = key;
    this.id = id;
  }

  public IN getWrapped() {
    return wrapped;
  }

  public void setWrapped(IN wrapped) {
    this.wrapped = wrapped;
  }

  public KEY getKey() {
    return key;
  }

  public void setKey(KEY key) {
    this.key = key;
  }

  public ID getId() {
    return id;
  }

  public void setId(ID id) {
    this.id = id;
  }
}
