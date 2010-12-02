package ru.hh.rabbitmq.util;

import com.rabbitmq.client.Address;
import java.util.LinkedList;
import java.util.List;

public class Addresses {
  public static Address[] split(String hosts, int port) {
    String[] splitHosts = hosts.split("\\s*,\\s*");
    if (splitHosts.length == 0) {
      throw new IllegalArgumentException("no rabbitmq hosts specified in : " + hosts);
    }
    Address[] addresses = new Address[splitHosts.length];
    for(int i = 0; i < splitHosts.length; i++) {
      if (splitHosts[i].length() != 0 ) {
        addresses[i] = new Address(splitHosts[i], port);
      }
    }
    return addresses;
  }
}
