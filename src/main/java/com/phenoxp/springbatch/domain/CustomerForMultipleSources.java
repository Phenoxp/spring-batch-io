package com.phenoxp.springbatch.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.batch.item.ResourceAware;
import org.springframework.core.io.Resource;

import java.time.LocalDate;
import java.util.Objects;

@Getter
@Setter
@Builder
@NoArgsConstructor
public class CustomerForMultipleSources implements ResourceAware {
    private long id;
    private String firstName;
    private String lastName;
    private LocalDate birthDate;
    private Resource resource;

    public CustomerForMultipleSources(long id, String firstName, String lastName, LocalDate birthDate) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthDate = birthDate;
    }

    public CustomerForMultipleSources(long id, String firstName, String lastName, LocalDate birthDate, Resource resource) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthDate = birthDate;
        this.resource = resource;
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CustomerForMultipleSources customer = (CustomerForMultipleSources) o;
        return id == customer.id &&
                Objects.equals(firstName, customer.firstName) &&
                Objects.equals(lastName, customer.lastName) &&
                Objects.equals(birthDate, customer.birthDate);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, firstName, lastName, birthDate);
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", birthDate=" + birthDate +
                ", resource=" + resource +
                '}';
    }
}
