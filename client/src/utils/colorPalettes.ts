// Add this to a utils file, e.g., src/utils/colorPalettes.ts

/**
 * Creates a cubehelix color palette similar to seaborn's
 * @param count Number of colors to generate
 * @param reverse Whether to reverse the color order
 * @returns Array of hex color strings
 */
export function getCubehelixPalette(count: number, reverse: boolean = false): string[] {
    // Approximated cubehelix palette colors (similar to sns.cubehelix_palette)
    const baseColors = [
      '#1a1530', '#25214d', '#2c2e69', '#2f3f84', '#2d5199', '#2863a7', 
      '#2376b3', '#1e88ba', '#1c9cbc', '#24aeba', '#35bfb4', '#50cea9', 
      '#73dc9b', '#9be68c', '#c5ec7d', '#f0ef77'
    ];
    
    // Calculate the proper range of colors based on count
    const result: string[] = [];
    if (count <= 1) {
      return [baseColors[0]];
    }
    
    for (let i = 0; i < count; i++) {
      const index = Math.floor(i * (baseColors.length - 1) / (count - 1));
      result.push(baseColors[index]);
    }
    
    return reverse ? result.reverse() : result;
}