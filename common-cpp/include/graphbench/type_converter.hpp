#pragma once

#include <string>
#include <any>
#include <stdexcept>
#include <sstream>
#include <algorithm>
#include <cctype>

namespace graphbench {

/**
 * Type enumeration for property types.
 */
enum class PropertyType;

/**
 * Utility class for converting values between different types.
 * Used for converting CSV strings and JSON values to the correct C++ types.
 */
class TypeConverter {
public:
    /**
     * Convert a String value to the appropriate C++ type based on target type.
     * Used when loading data from CSV files.
     *
     * @param value String value from CSV
     * @param targetType Target PropertyType
     * @return std::any containing the converted value, or empty if value is null/empty
     */
    static std::any convertFromString(const std::string& value, PropertyType targetType) {
        if (value.empty()) {
            return std::any();
        }

        try {
            switch (targetType) {
                case PropertyType::INTEGER:
                    return std::stoi(value);
                case PropertyType::LONG:
                    return std::stoll(value);
                case PropertyType::FLOAT:
                    return std::stof(value);
                case PropertyType::DOUBLE:
                    return std::stod(value);
                case PropertyType::BOOLEAN:
                    return parseBoolean(value);
                case PropertyType::STRING:
                default:
                    return value;
            }
        } catch (const std::exception&) {
            // If conversion fails, fall back to String
            return value;
        }
    }

    /**
     * Convert a double to the appropriate C++ numeric type.
     * Used when converting JSON numbers (typically double) to the correct type.
     *
     * @param num Number value (typically double from JSON)
     * @param targetType Target PropertyType
     * @return std::any containing the converted number
     */
    static std::any convertFromDouble(double num, PropertyType targetType) {
        switch (targetType) {
            case PropertyType::INTEGER:
                return static_cast<int>(num);
            case PropertyType::LONG:
                return static_cast<int64_t>(num);
            case PropertyType::FLOAT:
                return static_cast<float>(num);
            case PropertyType::DOUBLE:
                return num;
            default:
                return num;
        }
    }

    /**
     * Convert an integer to the appropriate C++ numeric type.
     */
    static std::any convertFromInt(int64_t num, PropertyType targetType) {
        switch (targetType) {
            case PropertyType::INTEGER:
                return static_cast<int>(num);
            case PropertyType::LONG:
                return num;
            case PropertyType::FLOAT:
                return static_cast<float>(num);
            case PropertyType::DOUBLE:
                return static_cast<double>(num);
            default:
                return num;
        }
    }

    /**
     * Convert a query value (from JSON) to the correct type based on metadata.
     * JSON deserializes numbers as double or int64_t, so we need to convert them to the correct type.
     *
     * @param value Value from JSON (std::any containing String, double, int64_t, bool, or empty)
     * @param targetType Target PropertyType from metadata
     * @return std::any containing the converted value
     */
    static std::any convertQueryValue(const std::any& value, PropertyType targetType) {
        if (!value.has_value()) {
            return std::any();
        }

        // Try to extract as different types and convert
        try {
            // Try as string
            if (value.type() == typeid(std::string)) {
                return convertFromString(std::any_cast<std::string>(value), targetType);
            }

            // Try as double
            if (value.type() == typeid(double)) {
                return convertFromDouble(std::any_cast<double>(value), targetType);
            }

            // Try as int64_t
            if (value.type() == typeid(int64_t)) {
                return convertFromInt(std::any_cast<int64_t>(value), targetType);
            }

            // Try as int
            if (value.type() == typeid(int)) {
                return convertFromInt(std::any_cast<int>(value), targetType);
            }

            // Try as bool
            if (value.type() == typeid(bool)) {
                if (targetType == PropertyType::BOOLEAN) {
                    return value;
                }
                // Convert bool to other types if needed
                bool boolVal = std::any_cast<bool>(value);
                return convertFromInt(boolVal ? 1 : 0, targetType);
            }

            // If we can't convert, return as-is
            return value;

        } catch (const std::bad_any_cast&) {
            // If cast fails, return as-is
            return value;
        }
    }

    /**
     * Convert std::any to a specific type with error handling.
     * Returns default value if conversion fails.
     */
    template<typename T>
    static T convertTo(const std::any& value, const T& defaultValue = T()) {
        if (!value.has_value()) {
            return defaultValue;
        }

        try {
            return std::any_cast<T>(value);
        } catch (const std::bad_any_cast&) {
            return defaultValue;
        }
    }

    /**
     * Convert std::any to string representation.
     */
    static std::string toString(const std::any& value) {
        if (!value.has_value()) {
            return "";
        }

        try {
            if (value.type() == typeid(std::string)) {
                return std::any_cast<std::string>(value);
            }
            if (value.type() == typeid(int)) {
                return std::to_string(std::any_cast<int>(value));
            }
            if (value.type() == typeid(int64_t)) {
                return std::to_string(std::any_cast<int64_t>(value));
            }
            if (value.type() == typeid(float)) {
                return std::to_string(std::any_cast<float>(value));
            }
            if (value.type() == typeid(double)) {
                return std::to_string(std::any_cast<double>(value));
            }
            if (value.type() == typeid(bool)) {
                return std::any_cast<bool>(value) ? "true" : "false";
            }
        } catch (const std::bad_any_cast&) {
            // Fall through
        }

        return "";
    }

private:
    /**
     * Parse boolean from string.
     * Accepts: "true", "false", "1", "0", "yes", "no" (case-insensitive)
     */
    static bool parseBoolean(const std::string& value) {
        std::string lower = value;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

        if (lower == "true" || lower == "1" || lower == "yes") {
            return true;
        }
        if (lower == "false" || lower == "0" || lower == "no") {
            return false;
        }

        throw std::invalid_argument("Invalid boolean value: " + value);
    }
};

} // namespace graphbench
