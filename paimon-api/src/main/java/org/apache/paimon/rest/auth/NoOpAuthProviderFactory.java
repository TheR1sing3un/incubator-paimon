package org.apache.paimon.rest.auth;

import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

public class NoOpAuthProviderFactory implements AuthProviderFactory  {

  @Override
  public String identifier() {
    return AuthProviderEnum.NOOP.identifier();
  }

  @Override
  public AuthProvider create(Options options) {
    return new AuthProvider() {
      @Override
      public Map<String, String> mergeAuthHeader(Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        return new HashMap<>();
      }
    };
  }
}
