package lmbrd.zn.util;

import lmbrd.zn.util.TimeUtil.TimeGroupType;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.util.Date;

/**
 * $Id$
 * $URL$
 * User: light
 * Date: 9/5/11
 * Time: 1:24 PM
 */
@XmlAccessorType( XmlAccessType.FIELD )
public class TimeRange {
// ------------------------------ FIELDS ------------------------------

	public Date dateFrom;

	public Date dateTo;

	public TimeGroupType grouping;

// --------------------- GETTER / SETTER METHODS ---------------------

	public Date getDateFrom() {
		return this.dateFrom;
	}

	public TimeRange setDateFrom(Date dateFrom) {
		this.dateFrom = dateFrom;
		return this;
	}

	public TimeRange setDateFrom(long dateFrom) {
		this.dateFrom = new Date( dateFrom );
		return this;
	}

	public Date getDateTo() {
		return this.dateTo;
	}

	public TimeRange setDateTo(Date dateTo) {
		this.dateTo = dateTo;
		return this;
	}

	public TimeRange setDateTo(long dateTo) {
		this.dateTo = new Date( dateTo );
		return this;
	}

	public TimeGroupType getGrouping() {
		return this.grouping;
	}

	public TimeRange setGrouping(TimeGroupType grouping) {
		this.grouping = grouping;
		return this;
	}


	@Override
	public String toString() {
		return "TimeRange{" +
				"dateFrom=" + dateFrom +
				", dateTo=" + dateTo +
				", grouping=" + grouping +
				'}';
	}
}
