/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package part1producer.avro_serialization;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class Customer extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -2658841664518211605L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Customer\",\"namespace\":\"part1producer.avro_serialization\",\"fields\":[{\"name\":\"first_name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"First Name of Customer\"},{\"name\":\"last_name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Last Name of Customer\"},{\"name\":\"age\",\"type\":\"int\",\"doc\":\"Age at the time of registration\"},{\"name\":\"height\",\"type\":\"float\",\"doc\":\"Height at the time of registration in cm\"},{\"name\":\"weight\",\"type\":\"float\",\"doc\":\"Weight at the time of registration in kg\"},{\"name\":\"automated_email\",\"type\":\"boolean\",\"doc\":\"Field indicating if the email is automated\",\"default\":true}],\"version\":\"1\"}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Customer> ENCODER =
      new BinaryMessageEncoder<Customer>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Customer> DECODER =
      new BinaryMessageDecoder<Customer>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Customer> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Customer> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Customer> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Customer>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Customer to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Customer from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Customer instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static Customer fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /** First Name of Customer */
  public java.lang.String first_name;
  /** Last Name of Customer */
  public java.lang.String last_name;
  /** Age at the time of registration */
  public int age;
  /** Height at the time of registration in cm */
  public float height;
  /** Weight at the time of registration in kg */
  public float weight;
  /** Field indicating if the email is automated */
  public boolean automated_email;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Customer() {}

  /**
   * All-args constructor.
   * @param first_name First Name of Customer
   * @param last_name Last Name of Customer
   * @param age Age at the time of registration
   * @param height Height at the time of registration in cm
   * @param weight Weight at the time of registration in kg
   * @param automated_email Field indicating if the email is automated
   */
  public Customer(java.lang.String first_name, java.lang.String last_name, java.lang.Integer age, java.lang.Float height, java.lang.Float weight, java.lang.Boolean automated_email) {
    this.first_name = first_name;
    this.last_name = last_name;
    this.age = age;
    this.height = height;
    this.weight = weight;
    this.automated_email = automated_email;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return first_name;
    case 1: return last_name;
    case 2: return age;
    case 3: return height;
    case 4: return weight;
    case 5: return automated_email;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: first_name = value$ != null ? value$.toString() : null; break;
    case 1: last_name = value$ != null ? value$.toString() : null; break;
    case 2: age = (java.lang.Integer)value$; break;
    case 3: height = (java.lang.Float)value$; break;
    case 4: weight = (java.lang.Float)value$; break;
    case 5: automated_email = (java.lang.Boolean)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'first_name' field.
   * @return First Name of Customer
   */
  public java.lang.String getFirstName() {
    return first_name;
  }


  /**
   * Sets the value of the 'first_name' field.
   * First Name of Customer
   * @param value the value to set.
   */
  public void setFirstName(java.lang.String value) {
    this.first_name = value;
  }

  /**
   * Gets the value of the 'last_name' field.
   * @return Last Name of Customer
   */
  public java.lang.String getLastName() {
    return last_name;
  }


  /**
   * Sets the value of the 'last_name' field.
   * Last Name of Customer
   * @param value the value to set.
   */
  public void setLastName(java.lang.String value) {
    this.last_name = value;
  }

  /**
   * Gets the value of the 'age' field.
   * @return Age at the time of registration
   */
  public int getAge() {
    return age;
  }


  /**
   * Sets the value of the 'age' field.
   * Age at the time of registration
   * @param value the value to set.
   */
  public void setAge(int value) {
    this.age = value;
  }

  /**
   * Gets the value of the 'height' field.
   * @return Height at the time of registration in cm
   */
  public float getHeight() {
    return height;
  }


  /**
   * Sets the value of the 'height' field.
   * Height at the time of registration in cm
   * @param value the value to set.
   */
  public void setHeight(float value) {
    this.height = value;
  }

  /**
   * Gets the value of the 'weight' field.
   * @return Weight at the time of registration in kg
   */
  public float getWeight() {
    return weight;
  }


  /**
   * Sets the value of the 'weight' field.
   * Weight at the time of registration in kg
   * @param value the value to set.
   */
  public void setWeight(float value) {
    this.weight = value;
  }

  /**
   * Gets the value of the 'automated_email' field.
   * @return Field indicating if the email is automated
   */
  public boolean getAutomatedEmail() {
    return automated_email;
  }


  /**
   * Sets the value of the 'automated_email' field.
   * Field indicating if the email is automated
   * @param value the value to set.
   */
  public void setAutomatedEmail(boolean value) {
    this.automated_email = value;
  }

  /**
   * Creates a new Customer RecordBuilder.
   * @return A new Customer RecordBuilder
   */
  public static part1producer.avro_serialization.Customer.Builder newBuilder() {
    return new part1producer.avro_serialization.Customer.Builder();
  }

  /**
   * Creates a new Customer RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Customer RecordBuilder
   */
  public static part1producer.avro_serialization.Customer.Builder newBuilder(part1producer.avro_serialization.Customer.Builder other) {
    if (other == null) {
      return new part1producer.avro_serialization.Customer.Builder();
    } else {
      return new part1producer.avro_serialization.Customer.Builder(other);
    }
  }

  /**
   * Creates a new Customer RecordBuilder by copying an existing Customer instance.
   * @param other The existing instance to copy.
   * @return A new Customer RecordBuilder
   */
  public static part1producer.avro_serialization.Customer.Builder newBuilder(part1producer.avro_serialization.Customer other) {
    if (other == null) {
      return new part1producer.avro_serialization.Customer.Builder();
    } else {
      return new part1producer.avro_serialization.Customer.Builder(other);
    }
  }

  /**
   * RecordBuilder for Customer instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Customer>
    implements org.apache.avro.data.RecordBuilder<Customer> {

    /** First Name of Customer */
    private java.lang.String first_name;
    /** Last Name of Customer */
    private java.lang.String last_name;
    /** Age at the time of registration */
    private int age;
    /** Height at the time of registration in cm */
    private float height;
    /** Weight at the time of registration in kg */
    private float weight;
    /** Field indicating if the email is automated */
    private boolean automated_email;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(part1producer.avro_serialization.Customer.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.first_name)) {
        this.first_name = data().deepCopy(fields()[0].schema(), other.first_name);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.last_name)) {
        this.last_name = data().deepCopy(fields()[1].schema(), other.last_name);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.age)) {
        this.age = data().deepCopy(fields()[2].schema(), other.age);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.height)) {
        this.height = data().deepCopy(fields()[3].schema(), other.height);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.weight)) {
        this.weight = data().deepCopy(fields()[4].schema(), other.weight);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.automated_email)) {
        this.automated_email = data().deepCopy(fields()[5].schema(), other.automated_email);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
    }

    /**
     * Creates a Builder by copying an existing Customer instance
     * @param other The existing instance to copy.
     */
    private Builder(part1producer.avro_serialization.Customer other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.first_name)) {
        this.first_name = data().deepCopy(fields()[0].schema(), other.first_name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.last_name)) {
        this.last_name = data().deepCopy(fields()[1].schema(), other.last_name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.age)) {
        this.age = data().deepCopy(fields()[2].schema(), other.age);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.height)) {
        this.height = data().deepCopy(fields()[3].schema(), other.height);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.weight)) {
        this.weight = data().deepCopy(fields()[4].schema(), other.weight);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.automated_email)) {
        this.automated_email = data().deepCopy(fields()[5].schema(), other.automated_email);
        fieldSetFlags()[5] = true;
      }
    }

    /**
      * Gets the value of the 'first_name' field.
      * First Name of Customer
      * @return The value.
      */
    public java.lang.String getFirstName() {
      return first_name;
    }


    /**
      * Sets the value of the 'first_name' field.
      * First Name of Customer
      * @param value The value of 'first_name'.
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder setFirstName(java.lang.String value) {
      validate(fields()[0], value);
      this.first_name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'first_name' field has been set.
      * First Name of Customer
      * @return True if the 'first_name' field has been set, false otherwise.
      */
    public boolean hasFirstName() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'first_name' field.
      * First Name of Customer
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder clearFirstName() {
      first_name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'last_name' field.
      * Last Name of Customer
      * @return The value.
      */
    public java.lang.String getLastName() {
      return last_name;
    }


    /**
      * Sets the value of the 'last_name' field.
      * Last Name of Customer
      * @param value The value of 'last_name'.
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder setLastName(java.lang.String value) {
      validate(fields()[1], value);
      this.last_name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'last_name' field has been set.
      * Last Name of Customer
      * @return True if the 'last_name' field has been set, false otherwise.
      */
    public boolean hasLastName() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'last_name' field.
      * Last Name of Customer
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder clearLastName() {
      last_name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'age' field.
      * Age at the time of registration
      * @return The value.
      */
    public int getAge() {
      return age;
    }


    /**
      * Sets the value of the 'age' field.
      * Age at the time of registration
      * @param value The value of 'age'.
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder setAge(int value) {
      validate(fields()[2], value);
      this.age = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'age' field has been set.
      * Age at the time of registration
      * @return True if the 'age' field has been set, false otherwise.
      */
    public boolean hasAge() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'age' field.
      * Age at the time of registration
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder clearAge() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'height' field.
      * Height at the time of registration in cm
      * @return The value.
      */
    public float getHeight() {
      return height;
    }


    /**
      * Sets the value of the 'height' field.
      * Height at the time of registration in cm
      * @param value The value of 'height'.
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder setHeight(float value) {
      validate(fields()[3], value);
      this.height = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'height' field has been set.
      * Height at the time of registration in cm
      * @return True if the 'height' field has been set, false otherwise.
      */
    public boolean hasHeight() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'height' field.
      * Height at the time of registration in cm
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder clearHeight() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'weight' field.
      * Weight at the time of registration in kg
      * @return The value.
      */
    public float getWeight() {
      return weight;
    }


    /**
      * Sets the value of the 'weight' field.
      * Weight at the time of registration in kg
      * @param value The value of 'weight'.
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder setWeight(float value) {
      validate(fields()[4], value);
      this.weight = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'weight' field has been set.
      * Weight at the time of registration in kg
      * @return True if the 'weight' field has been set, false otherwise.
      */
    public boolean hasWeight() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'weight' field.
      * Weight at the time of registration in kg
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder clearWeight() {
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'automated_email' field.
      * Field indicating if the email is automated
      * @return The value.
      */
    public boolean getAutomatedEmail() {
      return automated_email;
    }


    /**
      * Sets the value of the 'automated_email' field.
      * Field indicating if the email is automated
      * @param value The value of 'automated_email'.
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder setAutomatedEmail(boolean value) {
      validate(fields()[5], value);
      this.automated_email = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'automated_email' field has been set.
      * Field indicating if the email is automated
      * @return True if the 'automated_email' field has been set, false otherwise.
      */
    public boolean hasAutomatedEmail() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'automated_email' field.
      * Field indicating if the email is automated
      * @return This builder.
      */
    public part1producer.avro_serialization.Customer.Builder clearAutomatedEmail() {
      fieldSetFlags()[5] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Customer build() {
      try {
        Customer record = new Customer();
        record.first_name = fieldSetFlags()[0] ? this.first_name : (java.lang.String) defaultValue(fields()[0]);
        record.last_name = fieldSetFlags()[1] ? this.last_name : (java.lang.String) defaultValue(fields()[1]);
        record.age = fieldSetFlags()[2] ? this.age : (java.lang.Integer) defaultValue(fields()[2]);
        record.height = fieldSetFlags()[3] ? this.height : (java.lang.Float) defaultValue(fields()[3]);
        record.weight = fieldSetFlags()[4] ? this.weight : (java.lang.Float) defaultValue(fields()[4]);
        record.automated_email = fieldSetFlags()[5] ? this.automated_email : (java.lang.Boolean) defaultValue(fields()[5]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Customer>
    WRITER$ = (org.apache.avro.io.DatumWriter<Customer>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Customer>
    READER$ = (org.apache.avro.io.DatumReader<Customer>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.first_name);

    out.writeString(this.last_name);

    out.writeInt(this.age);

    out.writeFloat(this.height);

    out.writeFloat(this.weight);

    out.writeBoolean(this.automated_email);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.first_name = in.readString();

      this.last_name = in.readString();

      this.age = in.readInt();

      this.height = in.readFloat();

      this.weight = in.readFloat();

      this.automated_email = in.readBoolean();

    } else {
      for (int i = 0; i < 6; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.first_name = in.readString();
          break;

        case 1:
          this.last_name = in.readString();
          break;

        case 2:
          this.age = in.readInt();
          break;

        case 3:
          this.height = in.readFloat();
          break;

        case 4:
          this.weight = in.readFloat();
          break;

        case 5:
          this.automated_email = in.readBoolean();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










