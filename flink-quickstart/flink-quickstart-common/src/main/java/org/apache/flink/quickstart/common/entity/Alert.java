package org.apache.flink.quickstart.common.entity;

import java.util.Objects;

/**
 * A simple alert.
 */
public class Alert {

	private long accountId;

	public long getAccountId() {
		return accountId;
	}

	public void setAccountId(long accountId) {
		this.accountId = accountId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Alert alert = (Alert) o;
		return accountId == alert.accountId;
	}

	@Override
	public int hashCode() {
		return Objects.hash(accountId);
	}

	@Override
	public String toString() {
		return "Alert{" +
			"accountId=" + accountId +
			'}';
	}
}
