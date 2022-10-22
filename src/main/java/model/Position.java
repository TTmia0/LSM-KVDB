package model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 位置信息
 */
@Data
@AllArgsConstructor
public class Position {
    /**
     * 起始位置
     */
    private long start;

    /**
     * 长度
     */
    private long length;
}
