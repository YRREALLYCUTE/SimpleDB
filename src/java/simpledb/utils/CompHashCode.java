package simpledb.utils;

import java.lang.reflect.Field;

public class CompHashCode {

    public static int compute(Class<?> aClass, Object o) {
        Field[] fields = aClass.getFields();
        int res = 17;
        for (Field field : fields) {
            if(field.getType() == int.class
                    || field.getType() == byte.class
                    || field.getType() == char.class
                    || field.getType() == short.class) {
                try {
                    res = res * 37 + (int)(field.get(o));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            else if(field.getType() == double.class) {
                long tmp = 0;
                try {
                    tmp = Double.doubleToLongBits(field.getDouble(o));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                int c = (int)(tmp ^ (tmp >>> 32));
                res = res * 37 + c;
            }
            else if(field.getType() == boolean.class) {
                int c = 0;
                try {
                    c = field.getBoolean(o) ? 1:0;
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                res = res * 37 + c;
            } else {
                try {
                    res = res * 37 + field.get(o).hashCode();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        return res;
    }

}
