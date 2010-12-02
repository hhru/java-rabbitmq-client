package ru.hh.rabbitmq.util;

import com.rabbitmq.client.Address;
import java.util.LinkedList;
import java.util.List;

public class Addresses {
  public static Address[] split(String hosts, int port) {
    List<Address> addresses = new LinkedList<Address>();
    for (String host : hosts.split("\\s*,\\s*")) {
      if (host.length() != 0 ) {
        addresses.add(new Address(host, port));
      }
    }
    if (addresses.size() == 0) {
      throw new IllegalArgumentException("no rabbitmq hosts specified in : " + hosts);
    }
    return addresses.toArray(new Address[addresses.size()]);
  }
}
