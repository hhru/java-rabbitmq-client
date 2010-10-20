package ru.hh.rabbitmq.simple;

import javax.xml.bind.annotation.XmlElement;

public class DummyJsonObject {
  @XmlElement
  public int id = 1;
  @XmlElement
  public String string = "oneone";

  @Override
  public boolean equals(Object obj) {
    DummyJsonObject dummy = (DummyJsonObject) obj;
    return dummy.id == this.id && dummy.string.equals(this.string);
  }
}
