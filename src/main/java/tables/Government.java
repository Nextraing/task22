package tables;

import java.util.Objects;

public class Government {

    private int id;
    private String formName;

    public Government() {

    }

    public Government(int id, String formName) {

        this.id = id;
        this.formName = formName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFormName() {
        return formName;
    }

    public void setFormName(String formName) {
        this.formName = formName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Government)) return false;
        Government that = (Government) o;
        return getId() == that.getId() &&
                Objects.equals(getFormName(), that.getFormName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getFormName());
    }

    @Override
    public String toString() {
        return "Government{" +
                "ID=" + id +
                ", FormName='" + formName + '\'' +
                '}';
    }
}
