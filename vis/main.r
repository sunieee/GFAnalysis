# https://github.com/schochastics/graphlayouts

# flexible radial layouts
# Radial Layout with Focal Node

library(igraphdata)
library(patchwork)
library(ggraph)
data("karate")

p1 <- ggraph(karate, layout = "focus", focus = 1) +
    draw_circle(use = "focus", max.circle = 3) +
    geom_edge_link0(edge_color = "black", edge_width = 0.3) +
    geom_node_point(aes(fill = as.factor(Faction)), size = 2, shape = 21) +
    scale_fill_manual(values = c("#8B2323", "#EEAD0E")) +
    theme_graph() +
    theme(legend.position = "none") +
    coord_fixed() +
    labs(title = "Focus on Mr. Hi")

# Save the plot
ggsave("graph.png", p1, width = 10, height = 10)

p2 <- ggraph(karate, layout = "focus", focus = 34) +
    draw_circle(use = "focus", max.circle = 4) +
    geom_edge_link0(edge_color = "black", edge_width = 0.3) +
    geom_node_point(aes(fill = as.factor(Faction)), size = 2, shape = 21) +
    scale_fill_manual(values = c("#8B2323", "#EEAD0E")) +
    theme_graph() +
    theme(legend.position = "none") +
    coord_fixed() +
    labs(title = "Focus on John A.")

p <- p1 + p2

p.save <- p + plot_annotation(title = "Radial Layout with Focal Node", theme = theme(plot.title = element_text(hjust = 0.5)))





data <- fromJSON("RData/2121939561.json")   # hanjiawei

data <- fromJSON("RData/2125104194.json")

# Create a data frame for nodes and edges
nodes <- as.data.frame(data$node)
edges <- as.data.frame(data$edge)

# Create an igraph object
g <- graph_from_data_frame(d=edges, vertices=nodes, directed=TRUE)

p1 <- ggraph(g, layout = "fr") +
      geom_edge_link(aes(edge_width = extendsProb), edge_color = "grey") +
      geom_node_point(aes(size = radius / 10, fill = color, colour=color, alpha=0.7)) +
      scale_edge_width(range = c(0.1, 1)) +  # 调整边的宽度范围
      scale_fill_identity() +  # 使用节点的颜色
      theme_graph() +
      theme(legend.position = "none") +
      coord_fixed()



# 将level属性设置为节点的层
V(g)$level <- nodes$level

# 计算Sugiyama布局
sugiyama_layout <- create_layout(g, layout = 'sugiyama', layers = V(g)$level)

# 绘制图形
p2 <- ggraph(g, layout = sugiyama_layout) +
      geom_edge_link(aes(edge_width = extendsProb), edge_color = "grey") +
      geom_node_point(aes(size = radius / 10, fill = color, colour = color, alpha = 0.7)) +
      scale_edge_width(range = c(0.1, 1)) +
      scale_fill_identity() +
      theme_graph() +
      theme(legend.position = "none") +
      coord_fixed()



# 计算背景圈的半径（基于最大节点level）
max_level <- max(nodes$level)
circle_radii <- seq(1, max_level)

# 使用ggraph绘制FR图
p <- ggraph(g, layout = "fr") +
      # 绘制背景圈
      draw_circle(aes(r = circle_radii), color = "grey", alpha = 0.5) +
      # 绘制边
      geom_edge_link(aes(edge_width = extendsProb), edge_color = "grey") +
      scale_edge_width(range = c(0.1, 1)) +  # 调整边的宽度范围
      # 绘制节点
      geom_node_point(aes(size = radius / 2, fill = color)) +
      scale_fill_identity() +  # 使用节点的颜色
      theme_graph() +
      theme(legend.position = "none") +
      coord_fixed()



p <- ggraph(g, layout = "focus", focus = 1) +
    draw_circle(use = "focus", max.circle = max_level) +
    geom_edge_link(aes(edge_width = extendsProb), edge_color = "grey") +
    scale_edge_width(range = c(0.1, 1)) +
    geom_node_point(aes(size = radius / 2, fill = color)) +
    scale_fill_identity()
    theme_graph() +
    theme(legend.position = "none") +
    coord_fixed()


    # geom_node_point(aes(fill = as.factor(Faction)), size = 2, shape = 21) +
    # scale_fill_manual(values = c("#8B2323", "#EEAD0E")) +