package data.typed;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class JobHistoryEntry {
    private final Position position;
    private final Employer employer;
    private final int duration;

    public JobHistoryEntry(Position position, Employer employer, int duration) {
        this.position = position;
        this.employer = employer;
        this.duration = duration;
    }

    public Position getPosition() {
        return position;
    }

    public Employer getEmployer() {
        return employer;
    }

    public int getDuration() {
        return duration;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("position", position)
                .append("employer", employer)
                .append("duration", duration)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        JobHistoryEntry that = (JobHistoryEntry) o;

        return new EqualsBuilder()
                .append(duration, that.duration)
                .append(position, that.position)
                .append(employer, that.employer)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(position)
                .append(employer)
                .append(duration)
                .toHashCode();
    }
}
