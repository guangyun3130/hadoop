/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.authentication;

import java.io.*;
import java.net.*;
import java.time.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.*;
import org.apache.hadoop.fs.azurebfs.extensions.*;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;

/**
 * AuthorizationStatus maintains the status of Authorization and also SAS
 * token if authorizer is providing it.
 */
public class AuthorizationStatus {
  private boolean isAuthorized;
  private final Map<URI, SasTokenData> sasTokenMap;

  public AuthorizationStatus() {
    sasTokenMap = new HashMap<>();
  }

  /**
   * Fetch SAS token from the specific storePath URI.
   * @param storePathUri
   * @return SAS token queryparam string.
   */
  public String getSasTokenQuery(URI storePathUri) {
    if (sasTokenMap.containsKey(storePathUri)) {
      SasTokenData sasTokenData = sasTokenMap.get(storePathUri);
      if (isValidSas(sasTokenData)) {
        return sasTokenData.sasToken;
      }
    }

    return null;
  }

  /**
   * Updates sasTokenMap from the AuthorizationResult received from Authorizer.
   * Also update the refresh interval for each SAS token.
   *
   * @param authorizationResource
   * @param authResult - Authorizer AuthorizationResult.
   */
  public void setSasToken(AuthorizationResource[] authorizationResource,
      AuthorizationResult authResult)
      throws AbfsAuthorizationException, AbfsAuthorizerUnhandledException {

    AuthorizationResourceResult[] resourceResult = authResult
        .getAuthResourceResult();

    int i = 0;
    for (AuthorizationResourceResult singleResourceAuth : resourceResult) {
      // First check if the requested resource matches the resource
      // for which authToken is returned
      AuthorizationResource authorizationRequestedForResource =
          authorizationResource[i];

      if (singleResourceAuth == null) {
        throw new AbfsAuthorizationException("Invalid authorization "
            + "response. Null resource Authorization result");
      }

      if (singleResourceAuth.getStorePathUri() == null) {
        throw new AbfsAuthorizationException("Invalid authorization "
            + "response. Resource Authorization result with Null storePath "
            + "URI");
      }

      if (singleResourceAuth.getAuthorizerAction() == null) {
        throw new AbfsAuthorizationException(String.format(
            "Invalid authorization response. Resource Authorization result "
                + "for %s has null authorizerAction",
            singleResourceAuth.getStorePathUri()));
      }

      if (singleResourceAuth.getAuthToken() == null) {
        throw new AbfsAuthorizationException(String.format(
            "Invalid authorization response. Resource Authorization result "
                + "for %s for authorize action %s has null SAS token",
            singleResourceAuth.getStorePathUri(),
            singleResourceAuth.getAuthorizerAction()));
      }

      if (!singleResourceAuth.getStorePathUri().toString()
          .equals(authorizationRequestedForResource.getStorePathUri().toString())
          || !singleResourceAuth.getAuthorizerAction().equalsIgnoreCase(
          authorizationRequestedForResource.getAuthorizerAction())) {

        throw new AbfsAuthorizationException(String.format(
            "Mismatch in requested resource authorization action to received."
                + " Requested %s-%s, Received %s-%s",
            authorizationRequestedForResource.getStorePathUri().toString(),
            authorizationRequestedForResource.getAuthorizerAction(),
            singleResourceAuth.getStorePathUri().toString(),
            singleResourceAuth.getAuthorizerAction()));
      }

      SasTokenData authToken = new SasTokenData();

      // By default SASToken will be set for refresh
      // DEFAULT_SAS_REFRESH_INTERVAL_BEFORE_EXPIRY (5 mins) seconds before
      // expiry
      // If the sas token is short lived and below 5 mins, set the sas token
      // refresh to be half time before expiry
      try {
        authToken.sasExpiryTime = getSasExpiryDateTime(
            singleResourceAuth.getAuthToken());
      } catch (UnsupportedEncodingException e) {
        throw new AbfsAuthorizerUnhandledException(e);
      }

      authToken.sasToken = singleResourceAuth.getAuthToken();
      long durationToExpiryInSec = (
          Duration.between(Instant.now(), authToken.sasExpiryTime).toMillis()
              / 1000);

      if (durationToExpiryInSec < 0) {
        throw new AbfsAuthorizerUnhandledException(new InvalidRequestException(
            String.format("SAS token received is already expired. Expiry:%s "
                    + "StorePathURI:%s AuthorizerAction: %s",
                authToken.sasExpiryTime.toString(),
                singleResourceAuth.getStorePathUri(),
                singleResourceAuth.getAuthorizerAction())));
      }

      if (durationToExpiryInSec < DEFAULT_SAS_REFRESH_INTERVAL_BEFORE_EXPIRY) {
        authToken.sasRefreshIntervalBeforeExpiryInSec =
            (int) durationToExpiryInSec / 2;
      }

      sasTokenMap.put(singleResourceAuth.getStorePathUri(), authToken);
    }
  }

  /**
   * Fetch SAS token expiry for a given SAS Token query URL.
   *
   * @return Time of SAS token expiry.
   */
  private Instant getSasExpiryDateTime(String sasToken)
      throws UnsupportedEncodingException {
    String decodedSASToken = URLDecoder.decode(sasToken, "UTF-8");
    int startIndex = decodedSASToken.indexOf("se");
    int endIndex = decodedSASToken.indexOf("&", startIndex);
    if (endIndex == -1) {
      endIndex = decodedSASToken.length();
    }
    String se = decodedSASToken.substring(decodedSASToken.indexOf("se") + 3,
        // remove se=
        endIndex);

    return Instant.parse(se);
  }

  /**
   * Check if SASTokenData is valid and or if it needs update.
   *
   * @return true if SAS token is valid, false otherwise.
   */
  public boolean isValidSas(SasTokenData sasTokenData) {
    String sasTokenQuery = sasTokenData.sasToken;

    if ((sasTokenQuery == null) || sasTokenQuery.isEmpty()) {
      // If there is no SAS
      return false;
    }

    Instant currentDateTime = Instant.now();

    // if expiry is within configured refresh interval,
    // SAS token needs update. Return status as invalid.
    return sasTokenData.sasExpiryTime.isAfter(currentDateTime
        .minusSeconds(sasTokenData.sasRefreshIntervalBeforeExpiryInSec));
  }

  /**
   * Fetches and checks SAS token for provided Store Path URI.
   *
   * @param storepathUri
   * @return true if the SAS token is still valid, else false.
   */
  public boolean isValidSas(URI storepathUri) {
    if (sasTokenMap.containsKey(storepathUri)) {
      return isValidSas(sasTokenMap.get(storepathUri));
    }

    return false;
  }

  public boolean isAuthorized() {
    return isAuthorized;
  }

  public void setAuthorized(boolean authorized) {
    isAuthorized = authorized;
  }
}